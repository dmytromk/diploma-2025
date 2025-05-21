import json
import os
from datetime import date, datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from utils.slack_callbacks import dag_failure_slack_alert

dag_id = "dag_gold_user"
current_date = date.today()
json_variables_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"variables/{dag_id}.json")

with open(json_variables_path, 'r') as j:
	variables = json.loads(j.read())

with DAG(
		dag_id=dag_id,
		start_date=datetime(2025, 1, 25),
		schedule=[Dataset("s3://prod-vmart-redshift-analytics/dataset/dag_bronze_mysql_user.txt")],
		max_active_runs=1,
		description='DAG for creating a gold layer for gold.dim_user table',
		tags=['prod', 'gold', 'marketing', 'users'],
		default_args={
			'owner': 'airflow',
			'retries': 3,
			'retry_delay': timedelta(minutes=5),
			"on_failure_callback": dag_failure_slack_alert
		},
		catchup=False
) as dag:

	start_operator = EmptyOperator(task_id="dag_start")

	create_silver_updated_users = SQLExecuteQueryOperator(
		task_id=f'create_silver_updated_users',
		sql=f"""
                SET query_group TO 'slow-queue';
				DROP TABLE IF EXISTS silver.product_users;
				CREATE TABLE silver.product_users AS
				WITH most_recent_user AS (
					SELECT
						CAST(id AS BIGINT) as user_id,
						name as user_name,
						gender,
						init_country as country,
						CAST(birth_day as DATE) as birth_day,
						birth_place,
						relationship as relationship_status,
						CASE
							WHEN utc IS NULL OR TRIM(utc) = '' THEN NULL
							ELSE 
								CASE
									WHEN utc >= 0 THEN 'UTC+' 
									ELSE 'UTC-' 
								END ||
								LPAD(ABS(CAST(utc AS INTEGER)) / 3600, 2, '0') || 
								':' || 
								LPAD((ABS(CAST(utc AS INTEGER)) % 3600) / 60, 2, '0')
						END as utc_timezone,
						updated_at
					FROM bronze.product_users as aggregated_users
                    WHERE 1 = 1
					QUALIFY row_number() OVER(PARTITION BY id ORDER BY updated_at DESC) = 1
				),
				most_recent_phone AS (
					SELECT
						CAST(CASE WHEN TRIM(user_id) = '' THEN NULL ELSE user_id END AS BIGINT) as user_id,
						number as phone_number,
						updated_at
					FROM bronze.product_phone as aggregated_phones
                    WHERE 1 = 1
					QUALIFY row_number() OVER(PARTITION BY user_id ORDER BY updated_at DESC) = 1
				),
				most_recent_email AS (
					SELECT
						CAST(CASE WHEN TRIM(user_id) = '' THEN NULL ELSE user_id END AS BIGINT) as user_id,
						active_email as email,
						updated_at
					FROM bronze.product_email_user as aggregated_emails
                    WHERE 1 = 1
					QUALIFY row_number() OVER(PARTITION BY user_id ORDER BY updated_at DESC) = 1
				)		
				SELECT 
					u.user_id,
					u.user_name,
					u.gender,
					u.country,
					u.birth_day,
					u.birth_place,
					u.relationship_status,
					u.utc_timezone,
					p.phone_number,
					e.email,
					FARMFINGERPRINT64(
						COALESCE(CAST(u.user_id AS VARCHAR), 'NULL') ||
						COALESCE(u.user_name, 'NULL') ||
						COALESCE(u.gender, 'NULL') ||
						COALESCE(u.country, 'NULL') ||
						COALESCE(CAST(u.birth_day AS VARCHAR), 'NULL') ||
						COALESCE(u.birth_place, 'NULL') ||
						COALESCE(u.relationship_status, 'NULL') ||
						COALESCE(CAST(u.utc_timezone AS VARCHAR), 'NULL') ||
						COALESCE(p.phone_number, 'NULL') ||
						COALESCE(e.email, 'NULL')
					) AS fingerprint
				FROM most_recent_user u
				LEFT JOIN most_recent_phone p 
					ON u.user_id = p.user_id
				LEFT JOIN most_recent_email e 
					ON u.user_id = e.user_id
				WHERE 
                    u.user_id IS NOT NULL AND (
						DATE(u.updated_at) >= TO_TIMESTAMP({variables["time_delta"] if variables["time_delta"] != 'AUTO' else str(current_date - timedelta(days=15))}, 'YYYY-MM-DD') OR
						DATE(p.updated_at) >= TO_TIMESTAMP({variables["time_delta"] if variables["time_delta"] != 'AUTO' else str(current_date - timedelta(days=15))}, 'YYYY-MM-DD') OR
						DATE(e.updated_at) >= TO_TIMESTAMP({variables["time_delta"] if variables["time_delta"] != 'AUTO' else str(current_date - timedelta(days=15))}, 'YYYY-MM-DD')
					);
			""",
		split_statements=True,
		conn_id='aws_redshift_cluster',
		autocommit=True
	)

	insert_new_gold_dim_user_scd = SQLExecuteQueryOperator(
		task_id=f'insert_new_gold_dim_user_scd',
		sql=f"""
				SET query_group TO 'fast-queue';
				INSERT INTO gold.dim_user
				SELECT 
					user_id,
					user_name, 
					email,
					gender, 
					country,
					birth_day,
					birth_place, 
					relationship_status, 						
					utc_timezone, 
					phone_number,
					fingerprint,
					CURRENT_TIMESTAMP AS valid_from,
					CAST('9999-01-01 00:00:00' AS DATETIME) AS valid_to,
					1 AS is_active,
					CASE 
						WHEN orgn.email LIKE '%gen.tech%' THEN TRUE
						ELSE FALSE
					END AS is_test
				FROM silver.product_users AS orgn					
				WHERE orgn.user_id NOT IN (SELECT DISTINCT user_id FROM gold.dim_user);
			""",
		split_statements=True,
		conn_id='aws_redshift_cluster',
		autocommit=True
	)

	insert_modified_gold_dim_user_scd = SQLExecuteQueryOperator(
		task_id=f'insert_modified_gold_dim_user_scd',
		sql=f"""
				SET query_group TO 'fast-queue';
				INSERT INTO gold.dim_user
				SELECT 
					orgn.user_id,
					orgn.user_name, 
					orgn.email,
					orgn.gender, 
					orgn.country,
					orgn.birth_day,
					orgn.birth_place, 
					orgn.relationship_status, 						
					orgn.utc_timezone, 
					orgn.phone_number,
					orgn.fingerprint,
					CURRENT_TIMESTAMP AS valid_from,
					CAST('9999-01-01 00:00:00' AS DATETIME) AS valid_to,
					1 AS is_active,
					CASE 
						WHEN orgn.email LIKE '%@gen.tech%' THEN TRUE
						ELSE FALSE
					END AS is_test
				FROM silver.product_users AS orgn
				INNER JOIN gold.dim_user AS tgt
					ON orgn.user_id = tgt.user_id AND orgn.fingerprint <> tgt.fingerprint AND tgt.is_active = TRUE;
			""",
		split_statements=True,
		conn_id='aws_redshift_cluster',
		autocommit=True
	)

	update_old_gold_dim_user_scd = SQLExecuteQueryOperator(
		task_id=f'update_old_gold_dim_user_scd',
		sql=f"""
				SET query_group TO 'fast-queue';
				UPDATE gold.dim_user AS tgt
				SET 
					is_active = FALSE,
					valid_to = orgn.valid_from - INTERVAL '1 SECOND'
				FROM (
					SELECT user_id, valid_from, fingerprint FROM gold.dim_user
				) AS orgn
				WHERE orgn.user_id = tgt.user_id AND 
					orgn.fingerprint <> tgt.fingerprint AND 
					orgn.valid_from > tgt.valid_from AND
					tgt.is_active = TRUE;
			""",
		split_statements=True,
		conn_id='aws_redshift_cluster',
		autocommit=True
	)

	end_operator = EmptyOperator(task_id="dag_end")

	start_operator >> \
	create_silver_updated_users >> \
	insert_new_gold_dim_user_scd >> \
	insert_modified_gold_dim_user_scd >> \
	update_old_gold_dim_user_scd >> \
	end_operator