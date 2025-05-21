import json
import os

from airflow import DAG, Dataset
from airflow.models import Connection
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

from utils.slack_callbacks import dag_failure_slack_alert

dag_id = "dag_bronze_facebook_ad_spend"
destination_folder = f"dag_runs/{dag_id}/{datetime.today().strftime('%Y-%m-%d')}"
json_variables_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"variables/{dag_id}.json")

with open(json_variables_path, "r") as j:
	variables = json.loads(j.read())

conn = Connection.get_connection_from_secrets("aws_secrets")

aws_access_key = conn.login
aws_secret_key = conn.password

load_starting_date = variables["datalake_load_starting_date"]

with DAG(
		dag_id=dag_id,
		start_date=datetime(2025, 3, 7),
		schedule="10 * * * *",
		max_active_runs=1,
		description="DAG for incrementally loading Facebook ad spends into Redshift",
		tags=["prod", "bronze", "marketing", "facebook", "marketing spends"],
		default_args={
			"owner": "airflow",
			"retries": 3,
			"retry_delay": timedelta(minutes=5),
			"on_failure_callback": dag_failure_slack_alert
		},
		catchup=False
) as dag:
	start_operator = EmptyOperator(task_id="dag_start")

	with TaskGroup(group_id="facebook_ads_to_redshift_load_group") as facebook_ads_to_redshift_load_group:
		"""
		This task is designed to extract reports from Solid API into S3 .json files using Lambda
		"""
		load_facebook_api_to_s3 = LambdaInvokeFunctionOperator(
			task_id=f"load_facebook_api_to_s3",
			function_name=variables["lambda_function"],
			payload=json.dumps({
				"s3_bucket": variables["bucket_id"],
				"s3_filepath": variables["source_folder"],
				"date_from": (datetime.now() - timedelta(days=(variables["api_load_timedelta"] - 1))).strftime("%Y-%m-%d")
					if variables["api_load_timedelta"] != 'AUTO' else datetime.now().strftime("%Y-%m-%d"),
				"date_to": datetime.now().strftime("%Y-%m-%d"),
				"account_ids": variables["account_ids"]
			}),
			botocore_config={
				"connect_timeout": 900,
				"read_timeout": 900,
				"tcp_keepalive": True
			}
		)

		truncate_staging_table = SQLExecuteQueryOperator(
			task_id=f"truncate_staging_table",
			sql=f"""
				SET query_group TO 'fast-queue';
				TRUNCATE TABLE staging.{variables['table_name']};
				""",
			split_statements=True,
			conn_id="aws_redshift_cluster",
			autocommit=True
		)

		load_s3_to_redshift_staging = SQLExecuteQueryOperator(
			task_id=f'load_s3_to_redshift_staging',
			sql="\n".join(
					f'''
					COPY vmart.staging.{variables['table_name']}
					FROM 's3://{variables["bucket_id"]}/{variables['source_folder']}/{year}/{month}/{day}/'
					credentials
					'aws_access_key_id={aws_access_key};aws_secret_access_key={aws_secret_key}'
					FORMAT AS JSON 'auto';
					'''
				for date_path in [
					(datetime.now() - timedelta(days=day_number)).strftime("%Y-%m-%d")
						for day_number in range(0, load_starting_date if load_starting_date != "AUTO" else 1)
				]
				for year, month, day in [date_path.split('-')]
			),
			conn_id='aws_redshift_cluster',
			split_statements = True,
			autocommit=True
		)

		delete_data_from_bronze_layer = SQLExecuteQueryOperator(
			task_id=f"delete_data_from_bronze_layer",
			sql=f"""
					SET query_group TO 'fast-queue';
					DELETE FROM bronze.{variables["table_name"]}
					WHERE ad_spend_sk IN (
						SELECT DISTINCT
							FARMFINGERPRINT64(
								CAST(FARMFINGERPRINT64(
                					COALESCE("ad_id", 'NULL') || 
                					COALESCE("campaign_id", 'NULL')) 
                					AS VARCHAR) ||							
            					COALESCE(CAST("date_stop" AS VARCHAR), 'NULL') ||							
            				COALESCE("country", 'NULL')	
							) AS ad_spend_sk
						FROM staging.{variables["table_name"]}
					);		
				""",
			split_statements=True,
			conn_id="aws_redshift_cluster",
			autocommit=True
		)

		insert_data_into_bronze_layer = SQLExecuteQueryOperator(
			task_id=f"insert_data_into_bronze_layer",
			sql=f"""	
				SET query_group TO 'fast-queue';			
				INSERT INTO bronze.{variables["table_name"]}
				(
					SELECT
						origin_table.*
					FROM (
						SELECT DISTINCT
							FARMFINGERPRINT64(
                                {' || '.join(['COALESCE(CAST("{0}" AS VARCHAR), {1})'.format(target_column, "'NULL'") for target_column in variables["target_column_names"]])}
                            ) AS fingerprint,
							FARMFINGERPRINT64(
								CAST(FARMFINGERPRINT64(
									COALESCE("ad_id", 'NULL') || 
									COALESCE("campaign_id", 'NULL')) 
									AS VARCHAR) ||							
								COALESCE(CAST("date_stop" AS VARCHAR), 'NULL') ||							
								COALESCE("country", 'NULL')						
							) AS ad_spend_sk,
							{", ".join([f'"{target_column}"' if target_column != 'date_stop' else f'"{target_column}"::timestamp AS "{target_column}"' for target_column in variables["target_column_names"]])}
						FROM
							staging.{variables["table_name"]}
					) AS origin_table
					WHERE origin_table.fingerprint NOT IN (SELECT DISTINCT fingerprint FROM bronze.{variables["table_name"]} AS target_table)
					ORDER BY origin_table.date_stop
				);
			""",
			split_statements=True,
			conn_id="aws_redshift_cluster",
			autocommit=True
		)

		load_facebook_api_to_s3 >> truncate_staging_table >> load_s3_to_redshift_staging >> delete_data_from_bronze_layer >> insert_data_into_bronze_layer

	end_operator = EmptyOperator(task_id="dag_end", outlets=[Dataset(f"s3://prod-vmart-redshift-analytics/dataset/{dag_id}.txt")])

	start_operator >> facebook_ads_to_redshift_load_group >> end_operator
