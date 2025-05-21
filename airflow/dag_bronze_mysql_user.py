import json
import os
from datetime import date, datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.utils.task_group import TaskGroup

from utils.slack_callbacks import dag_failure_slack_alert

dag_id = "dag_bronze_mysql_user"
current_date = date.today()
json_variables_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"variables/{dag_id}.json")

with open(json_variables_path, 'r') as j:
	variables = json.loads(j.read())

temp_dir = f"tmp/{dag_id}/{current_date}"
bucket_id = variables['bucket_id']


def get_secrets():
	from airflow.models import Variable

	return {
		"userdata-mysql-secrets": Variable.get("userdata-mysql-user-connection", default_var="undefined", deserialize_json=True)
	}

def load_mysql_to_s3_callback(source_table_dataset: str, source_table_name: str, source_table_columns: list[str],
							  source_incremental_column: str, source_incremental_date: str,
							  s3_bucket: str, s3_filepath: str):
	import csv
	from mysql.connector import connect
	from airflow.providers.amazon.aws.hooks.s3 import S3Hook


	s3_hook = S3Hook(aws_conn_id="aws_default")
	mysql_credentials = {
		"host": get_secrets()["userdata-mysql-secrets"]["host"],
		"port": get_secrets()["userdata-mysql-secrets"]["port"],
		"user": get_secrets()["userdata-mysql-secrets"]["user"],
		"password": get_secrets()["userdata-mysql-secrets"]["password"],
		"database": get_secrets()["userdata-mysql-secrets"]["database"]
	}

	chunk_size = 50000

	with connect(auth_plugin="mysql_native_password", **mysql_credentials) as mysql_conn:
		import time

		mysql_cursor = mysql_conn.cursor()
		print("Connection to MySQL established successfully")

		column_list = ', '.join([f'CAST(`{column}` AS CHAR(512)) AS "{column}"' for column in source_table_columns])
		filter_statement = f"WHERE {source_incremental_column} >= '{source_incremental_date}'"
		extract_query = f"""
						SELECT {column_list}
						FROM {source_table_dataset}.{source_table_name}
						{filter_statement if source_incremental_column != 'N/A' else ''}
						LIMIT 1000000000;
					"""

		print(f"Running of '{extract_query}'...")
		start_time = time.time()
		mysql_cursor.execute(extract_query)
		end_time = time.time()
		print(f"Query succeeded!")
		print(f"Running of '{extract_query}' against MySQL took {end_time - start_time} seconds")

		pathname = f'{temp_dir}/{source_table_dataset}_{source_table_name}'
		os.makedirs(pathname, exist_ok=True)
		print(pathname)

		start_time = time.time()
		while True:
			rows = mysql_cursor.fetchmany(chunk_size)
			if not rows:
				break
			
			index = int(mysql_cursor.rowcount / chunk_size)

			with open(f"{pathname}/{index}.csv", "w", newline='', encoding="utf-8") as output_file:
				writer = csv.writer(output_file)
				writer.writerows(rows)

			upload_s3_file(s3_hook, f"{pathname}/{index}.csv", f"{s3_filepath}/{index}.csv", s3_bucket, True)
			remove_temp_file(f"{pathname}/{index}.csv")
		end_time = time.time()

		remove_temp_directory(pathname)

		print(f"Data successfully uploaded in .csv format to S3!")
		print(f"Fetching {mysql_cursor.rowcount} rows took {end_time - start_time} seconds")

		mysql_cursor.close()

def upload_s3_file(hook, filename: str, dest_key: str, dest_bucket: str, replace: bool):
	s3_bucket, s3_key = hook.get_s3_bucket_key(dest_bucket, dest_key, "dest_bucket", "dest_key")
	hook.load_file(filename, s3_key, s3_bucket, replace)


def remove_temp_file(filepath: str):
	if os.path.exists(filepath):
		os.remove(filepath)
		print(f"File {filepath} was successfully removed!")
	else:
		raise Exception(f"{filepath} does not exist")


def remove_temp_directory(dir_name: str):
	import shutil

	shutil.rmtree(dir_name)
	print(f"Directory {dir_name} was successfully removed!")

with DAG(
		dag_id=dag_id,
		start_date=datetime(2025, 1, 15),
		schedule='15 */3 * * *',
		max_active_runs=1,
		description='DAG for incrementally loading MySQL user tables into Redshift',
		tags=['prod', 'bronze', 'marketing', 'users'],
		default_args={
			'owner': 'airflow',
			'retries': 3,
			'retry_delay': timedelta(minutes=5),
			"on_failure_callback": dag_failure_slack_alert
		},
		catchup=False
) as dag:

	start_operator = EmptyOperator(task_id="dag_start")

	with TaskGroup(group_id="mysql_userdata_to_redshift_load_group") as mysql_userdata_to_redshift_load_group:
		for mapping in variables["mappings"]:
			s3_filepath = f'{current_date}/{dag_id}/{mapping["source_table_dataset"]}_{mapping["source_table_name"]}'

			'''
	        This task is designed to remove previous CSV files in S3
            '''
			remove_s3_keys = S3DeleteObjectsOperator(
				task_id=f'remove_s3_keys_for_{mapping["source_table_dataset"]}_{mapping["source_table_name"]}',
				bucket=bucket_id,
				prefix=s3_filepath,
				aws_conn_id='aws_default'
			)

			'''
			This task is designed to extract data from MySQL into a local CSV file
			'''
			load_mysql_to_csv = PythonOperator(
				task_id=f'load_mysql_to_csv_for_{mapping["source_table_dataset"]}_{mapping["source_table_name"]}',
				python_callable=load_mysql_to_s3_callback,
				op_kwargs={
					"source_table_dataset": mapping["source_table_dataset"],
					"source_table_name": mapping["source_table_name"],
					"source_table_columns": mapping["target_column_names"],
					"source_incremental_column": mapping["source_incremental_column"],
					"source_incremental_date": mapping["source_incremental_date"] if mapping["source_incremental_date"] != 'AUTO' else str(current_date - timedelta(days=15)),
					"s3_bucket": bucket_id,
					"s3_filepath": s3_filepath
				}
			)

			'''
			This task is designed to truncate Staging layer table
			'''
			truncate_staging_table = SQLExecuteQueryOperator(
				task_id=f'truncate_staging_table_for_{mapping["source_table_dataset"]}_{mapping["source_table_name"]}',
				sql=f'''
					SET QUERY_GROUP TO 'fast-queue';
					TRUNCATE TABLE staging.{mapping["target_table_name"]};
					''',
				split_statements=True,
				conn_id='aws_redshift_cluster',
				autocommit=True
			)

			'''
			This task is designed to load data from S3 bucket to Redshift
			'''
			load_s3_to_redshift_staging = S3ToRedshiftOperator(
				task_id=f'load_s3_to_redshift_staging_for_{mapping["source_table_dataset"]}_{mapping["source_table_name"]}',
				table=f'staging.{mapping["target_table_name"]}',
				copy_options=["csv"],
				s3_bucket=bucket_id,
				s3_key=s3_filepath,
				schema=f'vmart',
				redshift_conn_id='aws_redshift_cluster',
				aws_conn_id='aws_default'
			)

			'''
			This task is designed to load data which is NOT in Bronze layer from Staging layer.
			Thus, there going to be no duplicate records in bronze layer.
			In process, surrogate key and fingerprint are generated.

			SQL Parameters Description:
			[all_business_key_columns] = [business_key_column_name_1] || [business_key_column_name_2] || ... || [business_key_column_name_n]
			[all_mutable_columns] = [mutable_column_name_1] || [mutable_column_name_2] || ... || [mutable_column_name_n]
			'''
			insert_data_into_bronze_layer = SQLExecuteQueryOperator(
                task_id=f'insert_data_into_bronze_layer_for_{mapping["source_table_dataset"]}_{mapping["source_table_name"]}',
                sql=f'''
                	SET QUERY_GROUP TO 'slow-queue';					
                    INSERT INTO {mapping["target_table_dataset"]}.{mapping["target_table_name"]}
                    (
                    	SELECT origin_table.*
                        FROM (
                        	SELECT DISTINCT
                        		FARMFINGERPRINT64(
									{' || '.join(['COALESCE("{0}", {1})'.format(column, "'NULL'") for column in mapping["target_column_names"]])}
								) AS fingerprint,
								{', '.join([f'"{column}"' if column != mapping["source_incremental_column"] else f'CAST("{column}" AS TIMESTAMP) AS "{column}"'
											for column in mapping["target_column_names"]])}
                            FROM
                            	staging.{mapping["target_table_name"]}
                            ) AS origin_table
                            WHERE origin_table.fingerprint NOT IN (SELECT DISTINCT fingerprint FROM {mapping["target_table_dataset"]}.{mapping["target_table_name"]} AS target_table)
                    );
                    ''',
                split_statements=True,
                conn_id='aws_redshift_cluster',
                autocommit=True
            )

			remove_s3_keys >> load_mysql_to_csv >> truncate_staging_table >> load_s3_to_redshift_staging >> insert_data_into_bronze_layer

	end_operator = EmptyOperator(task_id="dag_end", outlets=[Dataset(f"s3://prod-vmart-redshift-analytics/dataset/{dag_id}.txt")])

	start_operator >> mysql_userdata_to_redshift_load_group >> end_operator