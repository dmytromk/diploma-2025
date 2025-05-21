import datetime
import json
import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.task_group import TaskGroup

from utils.slack_callbacks import dag_failure_slack_alert

dag_id = "dag_reference_exchange_rate"
current_date = date.today()
json_variables_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"variables/{dag_id}.json")

with open(json_variables_path, 'r') as j:
	variables = json.loads(j.read())

temp_dir = f"/tmp/{dag_id}/"
bucket_id = variables['bucket_id']

start = datetime.today().strftime('%Y%m%d') if variables['start_date']=='AUTO' else variables['start_date']
end = datetime.today().strftime('%Y%m%d') if  variables['end_date']=='AUTO' else variables['end_date']

url_usd = f'https://bank.gov.ua/NBU_Exchange/exchange_site?start={start}&end={end}&valcode=usd&sort=exchangedate&order=desc&json'
url_eur = f'https://bank.gov.ua/NBU_Exchange/exchange_site?start={start}&end={end}&valcode=eur&sort=exchangedate&order=desc&json'
url_gbp = f'https://bank.gov.ua/NBU_Exchange/exchange_site?start={start}&end={end}&valcode=gbp&sort=exchangedate&order=desc&json'
url_mxn = f'https://bank.gov.ua/NBU_Exchange/exchange_site?start={start}&end={end}&valcode=mxn&sort=exchangedate&order=desc&json'

def create_temp_directory_callback(dir_name: str):
	os.makedirs(dir_name, exist_ok=True)
	print(f"Directory {dir_name} was successfully created!")


def load_api_to_csv_callback(output_file_path: str):
	import csv
	import requests

	with open(output_file_path, "w+") as output_file:
		writer = csv.writer(output_file)
		print("Requesting exchange rate for USD...")
		req_usd = requests.get(url_usd)
		print(url_usd)
		print("Requesting exchange rate for EUR...")
		req_eur = requests.get(url_eur)
		print("Requesting exchange rate for GBP...")
		req_gbp = requests.get(url_gbp)
		print("Requesting exchange rate for MXN...")
		req_mxn = requests.get(url_mxn)
		data = []
		for i in range(len(req_usd.json())):
			usd_to_eur = req_usd.json()[i]['rate'] / req_eur.json()[i]['rate']
			usd_to_mxn = req_usd.json()[i]['rate'] / req_mxn.json()[i]['rate']
			usd_to_gbp = req_usd.json()[i]['rate'] / req_gbp.json()[i]['rate']
			date = datetime.strptime(req_usd.json()[i]['exchangedate'], "%d.%m.%Y").strftime("%Y-%m-%d")
			print(date)
			data.append(
				[date, usd_to_eur, usd_to_gbp, usd_to_mxn]
			)
		writer.writerows(data)


def remove_temp_file_callback(file_path: str):
	if os.path.exists(file_path):
		os.remove(file_path)
		print(f"File {file_path} was successfully removed!")
	else:
		raise Exception(f"{file_path} does not exist")


def remove_temp_directory_callback(dir_name: str):
	import shutil

	shutil.rmtree(dir_name)
	print(f"Directory {dir_name} was successfully removed!")


with DAG(
		dag_id=dag_id,
		start_date=datetime(2024, 11, 7),
		schedule='20 4 * * *',
		max_active_runs=1,
		description='DAG for incrementally loading exchange rates into Redshift',
		tags=['prod', 'reference', 'marketing', 'master data'],
		default_args={
			'owner': 'airflow',
			'retries': 3,
			'retry_delay': timedelta(minutes=5),
			"on_failure_callback": dag_failure_slack_alert
		},
		catchup=False
) as dag:
	start_operator = EmptyOperator(task_id="dag_start")

	'''
	This task is designed to create temporary directory in "temp" to store temporary CSV file generated in "vertica_to_redshift_load_group" task group 
	'''
	create_temp_directory = PythonOperator(
		task_id="create_temp_directory",
		python_callable=create_temp_directory_callback,
		op_kwargs={
			"dir_name": temp_dir
		}
	)

	with TaskGroup(group_id="api_to_redshift_load_group") as api_to_redshift_load_group:

			'''
			This task is designed to create intermediate table in Vertica
			'''
			load_api_to_csv = PythonOperator(
				task_id=f'load_api_to_csv',
				python_callable=load_api_to_csv_callback,
				op_kwargs={
					"output_file_path": f'{temp_dir}/{dag_id}.csv'
				}
			)

			'''
			This task is designed to load CSV data file from Local Filesystem to S3 bucket
			'''
			load_local_csv_to_s3 = LocalFilesystemToS3Operator(
				task_id=f'load_local_csv_to_s3',
				filename=f'{temp_dir}/{dag_id}.csv',
				dest_key=f'{current_date}/{dag_id}.csv',
				dest_bucket=bucket_id,
				aws_conn_id='aws_default',
				replace=True
			)

			'''
			This task is designed to truncate Staging layer table
			'''
			truncate_staging_table = SQLExecuteQueryOperator(
				task_id=f'truncate_staging_table',
				sql=f'''
					SET QUERY_GROUP TO 'default';	
					TRUNCATE TABLE staging.{variables["target_table_name"]};
					''',
				conn_id='aws_redshift_cluster',
				split_statements=True,
				autocommit=True
			)

			'''
			This task is designed to load data from S3 bucket to Redshift
			'''
			load_s3_to_redshift_staging = S3ToRedshiftOperator(
				task_id=f'load_s3_to_redshift_staging',
				table=f'staging.{variables["target_table_name"]}',
				copy_options=["CSV", "DELIMITER ','"],
				s3_bucket=bucket_id,
				s3_key=f'{current_date}/{dag_id}.csv',
				schema=f'vmart',
				redshift_conn_id='aws_redshift_cluster',
				aws_conn_id='aws_default'
			)

			'''
			This task is designed to load data which is NOT in Reference layer from Staging layer.
			Thus, there going to be no duplicate records in reference layer.
			In process, fingerprint is generated.
			'''
			insert_data_into_reference_layer = SQLExecuteQueryOperator(
				task_id=f'insert_data_into_reference_layer',
				sql=f'''
				    SET QUERY_GROUP TO 'slow-queue';					
					INSERT INTO {variables["target_table_dataset"]}.{variables["target_table_name"]}
					(
						SELECT
							origin_table.*
						FROM (
							SELECT DISTINCT
								FARMFINGERPRINT64(
									{' || '.join(['COALESCE("{0}", {1})'.format(column, "'NULL'") for column in variables["target_column_names"]])}
								) AS fingerprint,
								CAST(date AS DATE) AS date,
								CAST(usd_to_eur AS FLOAT) AS usd_to_eur,
								CAST(usd_to_gbp AS FLOAT) AS usd_to_gbp,
								CAST(usd_to_mxn AS FLOAT) AS usd_to_mxn
							FROM
								staging.{variables["target_table_name"]}
						) AS origin_table
						WHERE origin_table.fingerprint NOT IN (SELECT DISTINCT fingerprint FROM {variables["target_table_dataset"]}.{variables["target_table_name"]} AS target_table)
					);
				''',
				conn_id='aws_redshift_cluster',
				split_statements=True,
				autocommit=True
			)

			load_api_to_csv >> load_local_csv_to_s3 >> truncate_staging_table >> load_s3_to_redshift_staging >> insert_data_into_reference_layer

	end_operator = EmptyOperator(task_id="dag_end")

	start_operator >> create_temp_directory >> api_to_redshift_load_group >> end_operator