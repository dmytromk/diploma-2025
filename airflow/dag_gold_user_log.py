import json
import os
from datetime import date, datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

from utils.slack_callbacks import dag_failure_slack_alert

dag_id = "dag_gold_user_log"
current_date = date.today()
json_variables_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"variables/{dag_id}.json")

with open(json_variables_path, 'r') as j:
	variables = json.loads(j.read())

with DAG(
		dag_id=dag_id,
		start_date=datetime(2024, 12, 24),
		schedule=[Dataset("s3://prod-vmart-redshift-analytics/dataset/dag_bronze_webhook_user_touch_log.txt")],
		max_active_runs=1,
		description='DAG for creating a gold layer for gold.fact_user_log and gold.dim_non_paid_source tables',
		tags=['prod', 'gold', 'marketing', 'users'],
		default_args={
			'owner': 'airflow',
			"on_failure_callback": dag_failure_slack_alert
		},
		catchup=False
) as dag:

	start_operator = EmptyOperator(task_id="dag_start")

	create_silver_web_user_log = SQLExecuteQueryOperator(
		task_id=f'create_silver_web_user_log',
		sql=f'''
			SET query_group TO 'slow-queue';
			DROP TABLE IF EXISTS silver.web_user_log;
			CREATE TABLE silver.web_user_log AS
			(WITH paid_sources AS (
				SELECT utm_media_source as media_source
				FROM reference.paid_source_mapping
				WHERE is_paid = TRUE
			)
			SELECT
				event_id, 
				timestamp 'epoch' + CAST(event_time AS BIGINT)/1000 * interval '1 second' AS event_time,
				app_name,
				CASE 
					WHEN B.id IS NOT NULL THEN CAST(B.nebula_id AS VARCHAR)
					ELSE COALESCE(A.nebula_id, A.user_id)
				END AS user_id,
				CASE
					WHEN action = 'update'
						THEN 'web_remarketing'
					ELSE 'web_marketing'
				END AS event_name,
				CASE
					WHEN quiz IN ('love-compatibility', 'palmistry', 'shamans', 'karma',
									'feminine-archetypes', 'app-sub-intro', 'human-design', 'palmistry-intro',
									'attachment-style', 'attachment-style-zodiac', 'spirit-animal',
									'love-addiction', 'hypnotherapy-intro', 'goddess', 'relationship-anxiety',
									'life-control', 'loneliness', 'codependency')
						THEN quiz
					WHEN quiz IN ('app-subscription-4-7d-trial-per-day') THEN 'compatibility'
					WHEN quiz IN ('natal-chart-fb', 'natal-chart') THEN 'natal-chart'
					WHEN quiz IN ('witch-power', 'witch-power-tt') THEN 'witch-power'
					WHEN quiz IN ('witch-power-intro') THEN 'witch-power-intro'
					WHEN quiz IN ('moon-compatibility-tt', 'moon-compatibility') THEN 'moon-compatibility'
					WHEN quiz IN ('starseed') THEN 'star-seed'
					ELSE SUBSTRING(quiz, 0, 255)
				END AS funnel,
				CASE
					WHEN locale = 'ja' then 'jp'
					ELSE SUBSTRING(locale, 0, 255)
				END AS locale,
				COALESCE(
					CASE
						when media_source = 'organic' and seo_channel is not null then 'SEO'
						else SUBSTRING(media_source, 0, 255) 
					END, 
					'organic'
				) AS media_source,
				placement,
				CASE
					WHEN regexp_instr(user_agent, '(iPhone|iPad|iOS)') THEN 'ios'
					WHEN regexp_instr(user_agent, 'Android') THEN 'android'
				ELSE 'web'
				END AS platform,
				user_agent,
				CASE
					WHEN media_source IN (SELECT media_source FROM paid_sources) THEN campaign_id
					ELSE NULL
				END as campaign_id,
				CASE
					WHEN media_source IN ('google', 'bing', 'aqgoogle') THEN NULL
					WHEN media_source NOT IN (SELECT media_source FROM paid_sources) THEN NULL
					ELSE ad_id
				END as ad_id
			FROM bronze.web_user_log AS A
			LEFT JOIN reference.user_id_mapping AS B
				ON A.user_id = B.id
			WHERE (timestamp 'epoch' + CAST(event_time AS BIGINT)/1000 * interval '1 second')::DATE >= '{variables["web_time_delta"] if variables["web_time_delta"] != 'AUTO' else str(current_date - timedelta(days=15))}');
		''',
		split_statements=True,
		conn_id='aws_redshift_cluster',
		autocommit=True
	)

	create_silver_paid_user_log = SQLExecuteQueryOperator(
		task_id=f'create_silver_paid_user_log',
		sql=f"""SET query_group TO 'slow-queue';
				DROP TABLE IF EXISTS silver.paid_user_log;
				CREATE TABLE silver.paid_user_log AS
				SELECT
					FARMFINGERPRINT64(COALESCE(CAST(event_time as varchar), 'NULL') || COALESCE(app_name, 'NULL') || COALESCE(event_name, 'NULL') || COALESCE(user_id, 'NULL')) AS user_log_sk,
					FARMFINGERPRINT64(COALESCE(ad_id, 'NULL') || COALESCE(campaign_id, 'NULL')) AS ad_sk,
					NULL AS non_paid_source_sk,
					event_id, 
					user_id,
					ad_id,
					campaign_id,
					media_source,
					event_time,
					app_name,
					event_name,
					placement,
					wul.platform,
					user_agent
				FROM silver.web_user_log wul
				QUALIFY ROW_NUMBER() OVER(PARTITION BY user_log_sk ORDER BY event_time DESC) = 1;
		""",
		split_statements=True,
		conn_id='aws_redshift_cluster',
		autocommit=True
	)

	create_gold_fact_user_log = SQLExecuteQueryOperator(
		task_id=f'create_gold_fact_user_log',
		sql=f"""
			SET query_group TO 'fast-queue';
			INSERT INTO gold.fact_user_log
			SELECT 
				*, 
				CURRENT_TIMESTAMP AS insertion_time  
			FROM (
			SELECT
					user_log_sk,
					ad_sk,
					non_paid_source_sk::BIGINT as non_paid_source_sk,
					event_id, 
					user_id,
					event_time,
					app_name,
					event_name,
					placement,
					platform,
					user_agent,
					ad_id,
					campaign_id,
					media_source
				FROM silver.paid_user_log pul
				WHERE pul.user_log_sk NOT IN (SELECT DISTINCT ful.user_log_sk FROM gold.fact_user_log ful)
			) t
			ORDER BY event_time ASC;
		""",
		split_statements=True,
		conn_id='aws_redshift_cluster',
		autocommit=True
	)

	end_operator = EmptyOperator(task_id="dag_end", outlets=[Dataset(f"s3://prod-vmart-redshift-analytics/dataset/{dag_id}.txt")])

	start_operator >> \
	create_silver_web_user_log >> \
	create_silver_paid_user_log >> \
	create_gold_fact_user_log >> \
	end_operator
