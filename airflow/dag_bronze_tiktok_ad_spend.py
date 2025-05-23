import json
import os
from datetime import datetime, timedelta, date

from airflow import DAG, Dataset
from airflow.models import Connection
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from utils.slack_callbacks import dag_failure_slack_alert

dag_id = "dag_bronze_tiktok_ad_spend"
current_date = date.today()
destination_folder = f"dag_runs/{dag_id}/{datetime.today().strftime('%Y-%m-%d')}"
json_variables_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"variables/{dag_id}.json")

with open(json_variables_path, "r") as j:
	variables = json.loads(j.read())

conn = Connection.get_connection_from_secrets("aws_secrets")

aws_access_key = conn.login
aws_secret_key = conn.password

load_starting_date = variables["load_starting_date"]

with DAG(
		dag_id=dag_id,
		start_date=datetime(2025, 1, 28),
		schedule='10 * * * *',
		max_active_runs=1,
		description="DAG for incrementally loading TikTok ad spends into Redshift",
		tags=["prod", "bronze", "marketing", "tiktok", "marketing spends"],
		default_args={
			"owner": "airflow",
			"retries": 3,
			"retry_delay": timedelta(minutes=5),
			"on_failure_callback": dag_failure_slack_alert
		},
		catchup=False
) as dag:
	start_operator = EmptyOperator(task_id="dag_start")

	with TaskGroup(group_id="tiktok_ads_to_redshift_load_group") as tiktok_ads_to_redshift_load_group:
		"""
		This task is designed to extract reports from Tiktok API into S3 .json files using Lambda
		"""
		load_tiktok_api_data_to_s3 = LambdaInvokeFunctionOperator(
			task_id=f"load_tiktok_api_data_to_s3",
			function_name=variables["lambda_function"],
			payload=json.dumps({
				"s3_bucket": variables["bucket_id"],
				"s3_filepath": variables["source_folder"],
				"date_from": str(current_date - timedelta(days=variables["mappings"]["ad_spends"]["time_delta"])),
				"date_to": str(current_date),
				"advertiser_ids": variables["advertiser_ids"]
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
				SET QUERY_GROUP TO 'fast-queue';
				TRUNCATE TABLE staging.{variables["mappings"]["ad_spends"]["target_table_name"]};
				""",
			split_statements=True,
			conn_id="aws_redshift_cluster",
			autocommit=True
		)

		load_s3_to_redshift_staging = SQLExecuteQueryOperator(
			task_id=f'load_s3_to_redshift_staging',
			sql="\n".join(
				f'''
					COPY vmart.staging.{variables['mappings']['ad_spends']['target_table_name']}
					FROM 's3://{variables["bucket_id"]}/{variables['source_folder']}/{year}/{month}/{day}/'
					credentials
					'aws_access_key_id={aws_access_key};aws_secret_access_key={aws_secret_key}'
					FORMAT AS JSON 'auto';
					'''
				for date_path in [
					(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
					for i in range(0, (datetime.now() - datetime.strptime(
						load_starting_date if load_starting_date != "AUTO" else (datetime.now() - timedelta(
							days=variables['mappings']['ad_spends']['time_delta'])).strftime("%Y-%m-%d"),
						"%Y-%m-%d")).days + 1)
				]
				for year, month, day in [date_path.split('-')]
			),
			conn_id='aws_redshift_cluster',
			split_statements=True,
			autocommit=True
		)

		'''
		This task is designed to delete data from Bronze layer which is in new data in Staging layer.
		'''
		delete_data_from_bronze_layer = SQLExecuteQueryOperator(
			task_id=f"delete_data_from_bronze_layer",
			sql=f"""
					SET QUERY_GROUP TO 'fast-queue';
					DELETE FROM bronze.{variables["target_table_name"]}
						WHERE ad_spend_sk IN (
							SELECT DISTINCT
								FARMFINGERPRINT64(
									CAST(FARMFINGERPRINT64(
										COALESCE("ad_id", 'NULL') || 
										COALESCE("campaign_id", 'NULL')) 
										AS VARCHAR) ||							
									COALESCE(CAST("ad_spend_time" AS VARCHAR), 'NULL') ||							
								COALESCE("country_code", 'NULL')	
								) AS ad_spend_sk
							FROM staging.{variables["mappings"]["ad_spends"]["target_table_name"]}
						);
					""",
			split_statements=True,
			conn_id="aws_redshift_cluster",
			autocommit=True
		)

		insert_data_into_bronze_layer = SQLExecuteQueryOperator(
			task_id=f"insert_data_into_bronze_layer",
			sql=f"""			
				SET QUERY_GROUP TO 'slow-queue';					
				INSERT INTO {variables["target_table_dataset"]}.{variables["target_table_name"]}
				(
					SELECT
						origin_table.*
					FROM (
						SELECT DISTINCT
							FARMFINGERPRINT64(
								{' || '.join(['COALESCE(CAST("{0}" AS VARCHAR), {1})'.format(target_column, "'NULL'") for target_column in variables["target_columns"]])}
							) AS fingerprint,
							FARMFINGERPRINT64(
								CAST(FARMFINGERPRINT64(
									COALESCE("ad_id", 'NULL') ||
									COALESCE("campaign_id", 'NULL')) AS VARCHAR) ||
								COALESCE(CAST("ad_spend_time" AS VARCHAR), 'NULL') ||
								COALESCE("country_code", 'NULL')
							) AS ad_spend_sk,
							CAST(ad_spend_time AS DATE) as ad_spend_time,
							advertiser_id,
                            advertiser_name,
                            campaign_id,
                            campaign_name,
                            adset_id,
                            adset_name,
                            ad_id,
                            ad_name,
                            CAST(spend AS FLOAT),
                            'USD' as currency,
                            CAST(impressions AS INT) as impressions,
                            CAST(clicks AS INT) as clicks,
                            country_code,
                            CAST(video_watched_25 AS INT) as video_watched_25,
                            CAST(video_watched_50 AS INT) as video_watched_50,
                            CAST(video_watched_75 AS INT) as video_watched_75,
                            CAST(video_watched_100 AS INT) as video_watched_100,
                            CAST(video_watched_2s AS INT) as video_watched_2s,
                            CAST(average_video_play AS INT) as average_video_play,
                            media_source
						FROM
							staging.{variables["mappings"]["ad_spends"]["target_table_name"]}
					) AS origin_table
				WHERE fingerprint NOT IN (SELECT DISTINCT fingerprint FROM {variables["target_table_dataset"]}.{variables["target_table_name"]} AS target_table)
				);
			""",
			split_statements=True,
			conn_id="aws_redshift_cluster",
			autocommit=True
		)

		load_tiktok_api_data_to_s3 >> truncate_staging_table >> load_s3_to_redshift_staging >> delete_data_from_bronze_layer >> insert_data_into_bronze_layer

	end_operator = EmptyOperator(task_id="dag_end", outlets=[Dataset(f"s3://prod-vmart-redshift-analytics/dataset/{dag_id}.txt")])

	start_operator >> tiktok_ads_to_redshift_load_group >> end_operator
