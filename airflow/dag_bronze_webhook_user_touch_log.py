import json
import os
from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from utils.slack_callbacks import dag_failure_slack_alert

dag_id = "dag_bronze_webhook_user_touch_log"
json_variables_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"variables/{dag_id}.json")

with open(json_variables_path, 'r') as j:
	variables = json.loads(j.read())


def prepare_s3_objects_for_load_callback(load_starting_date, source_s3_path, target_s3_path, **kwargs):
	from airflow.providers.amazon.aws.hooks.s3 import S3Hook

	s3_hook = S3Hook(aws_conn_id='aws_default')
	source_bucket_id = variables['source_bucket_id']
	target_bucket_id = variables['target_bucket_id']

	load_dates = [
		(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
		for i in range(0, (datetime.now() - datetime.strptime(
			load_starting_date if load_starting_date != 'AUTO' else (datetime.now() - timedelta(days=3)).strftime(
				"%Y-%m-%d"), '%Y-%m-%d')).days + 1)
	]

	object_keys = s3_hook.list_keys(bucket_name=source_bucket_id)
	filtered_keys = []

	for load_date in load_dates:
		filtered_keys.extend([key for key in object_keys if f'{source_s3_path}/{load_date}' in key])

	if object_keys.__contains__(target_s3_path):
		s3_hook.delete_objects(
			bucket_name=target_bucket_id,
			keys=target_s3_path
		)

	for key in filtered_keys:
		destination_key = f"{target_s3_path}{'/'.join(folder for index, folder in enumerate(key.split('/')) if index not in [0, 1, 2])}"

		s3_hook.copy_object(
			source_bucket_name=source_bucket_id,
			source_bucket_key=key,
			dest_bucket_name=target_bucket_id,
			dest_bucket_key=destination_key
		)

with DAG(
		dag_id=dag_id,
		start_date=datetime(2025, 1, 28),
		schedule='10 * * * *',
		max_active_runs=1,
		description='DAG for incrementally loading Backend Webhook events',
		tags=['prod', 'bronze', 'marketing', 'webhook', 'users'],
		default_args={
			'owner': 'airflow',
			'retries': 3,
			'retry_delay': timedelta(minutes=5),
			"on_failure_callback": dag_failure_slack_alert
		},
		catchup=False
) as dag:
	start_operator = EmptyOperator(task_id="dag_start")

	with TaskGroup(group_id="s3_to_redshift_load_group") as s3_to_redshift_load_group:
		for mapping in variables["mappings"]:

			prepare_s3_objects_for_load = PythonOperator(
				task_id=f'prepare_s3_objects_for_loading_{mapping["target_table_name"]}',
				python_callable=prepare_s3_objects_for_load_callback,
				op_kwargs={
					"load_starting_date": mapping["load_starting_date"],
					"source_s3_path": mapping["source_s3_path"],
					"target_s3_path": f'dag_runs/{dag_id}/{mapping["target_table_name"]}/{datetime.today().strftime("%Y-%m-%d")}/'
				},
				provide_context=True,
			)

			truncate_staging_table = SQLExecuteQueryOperator(
				task_id=f'truncate_staging_table_for_{mapping["target_table_name"]}',
				sql=f'''
					SET QUERY_GROUP TO 'fast-queue';
					TRUNCATE TABLE staging.{mapping["target_table_name"]};
					''',
				split_statements=True,
				conn_id='aws_redshift_cluster',
				autocommit=True
			)

			load_s3_to_redshift_staging = S3ToRedshiftOperator(
				task_id=f'load_s3_to_redshift_staging_for_{mapping["target_table_name"]}',
				table=f'staging.{mapping["target_table_name"]}',
				copy_options=["FORMAT AS PARQUET", "SERIALIZETOJSON"],
				s3_bucket=variables['target_bucket_id'],
				s3_key=f'dag_runs/{dag_id}/{mapping["target_table_name"]}/{datetime.today().strftime("%Y-%m-%d")}/',
				schema=f'vmart',
				redshift_conn_id='aws_redshift_cluster',
				aws_conn_id='aws_default'
			)

			insert_data_into_bronze_layer = SQLExecuteQueryOperator(
				task_id=f'insert_data_into_bronze_layer_for_{mapping["target_table_name"]}',
				sql=f'''
					SET QUERY_GROUP TO 'slow-queue';				
					INSERT INTO bronze.{mapping["target_table_name"]}
					(
						SELECT
							origin_table.*
						FROM (
							SELECT DISTINCT
								FARMFINGERPRINT64(
									{' || '.join(['COALESCE(CAST("{0}" AS VARCHAR), {1})'.format(column, "'NULL'") for column in mapping["source_column_names"]])}
								) AS fingerprint,
								{', '.join([
									f'"{column}"' if column in mapping["source_column_names"]
									else (
										f"LEFT(JSON_EXTRACT_PATH_TEXT(event_data, '{column}', true), 512) AS {column}" if column == 'user_agent'
										else f"JSON_EXTRACT_PATH_TEXT(event_data, '{column}', true) AS {column}"
									)
									for column in mapping["target_column_names"]
								])}
							FROM
								staging.{mapping["target_table_name"]}
						) AS origin_table
						WHERE origin_table.fingerprint NOT IN (SELECT DISTINCT fingerprint FROM bronze.{mapping["target_table_name"]} AS target_table) AND
    						origin_table.event_id NOT IN (SELECT DISTINCT event_id FROM bronze.{mapping["target_table_name"]} AS target_table)
						ORDER BY {mapping["source_incremental_column"]} ASC
					);
				''',
				split_statements=True,
				conn_id='aws_redshift_cluster',
				autocommit=True
			)

			prepare_s3_objects_for_load >> truncate_staging_table >> load_s3_to_redshift_staging >> insert_data_into_bronze_layer

	end_operator = EmptyOperator(task_id="dag_end", outlets=[Dataset(f"s3://prod-vmart-redshift-analytics/dataset/{dag_id}.txt")])

	start_operator >> s3_to_redshift_load_group >> end_operator