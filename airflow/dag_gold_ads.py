import json
import os
from datetime import date, datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.task_group import TaskGroup

from utils.slack_callbacks import dag_failure_slack_alert

dag_id = "dag_gold_ads"
current_date = date.today()
json_variables_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"variables/{dag_id}.json")

with open(json_variables_path, 'r') as j:
	variables = json.loads(j.read())

with (DAG(
		dag_id=dag_id,
		start_date=datetime(2024, 12, 24),
		max_active_runs=1,
		schedule=[
			Dataset("s3://prod-vmart-redshift-analytics/dataset/dag_bronze_facebook_ad_spend.txt")
		],
		description='DAG for creating a gold layer for gold.dim_ad and gold.fact_ad_cost tables',
		tags=['prod', 'gold', 'marketing', 'marketing spends'],
		default_args={
			'owner': 'airflow',
			"on_failure_callback": dag_failure_slack_alert
		},
		catchup=False
) as dag):

	start_operator = EmptyOperator(task_id="dag_start")

	with TaskGroup(group_id="create_silver_marketing_cost_log") as create_silver_marketing_cost_log:
		create_table_silver_marketing_cost_log = SQLExecuteQueryOperator(
			task_id=f'create_table_silver_marketing_cost_log',
			sql=f"""
			SET query_group TO 'fast-queue';
			DROP TABLE IF EXISTS silver.marketing_cost_log;
			CREATE TABLE silver.marketing_cost_log 
				(
					event_time        varchar(255),
					country_code      varchar(255),
					media_source      varchar(255),
					advertiser_id     varchar(255),
					advertiser_name   varchar(512),
					campaign_id       varchar(255),
					campaign_name     varchar(512),
					adset_id          varchar(255),
					adset_name        varchar(512),
					ad_id             varchar(255),
					ad_name           varchar(512),
					ad_short_name     varchar(255),
					impressions       varchar(255),
					clicks            varchar(255),
					spend             varchar(255),
					currency          varchar(255),
					video_watched_25  varchar(255),
					video_watched_50  varchar(255),
					video_watched_75  varchar(255),
					video_watched_100 varchar(255),
					video_watched_2s  varchar(255),
					avg_play_time     varchar(255),
					app_name          varchar(255),
					platform          varchar(255)
				);
			""",
			split_statements=True,
			conn_id='aws_redshift_cluster',
			autocommit=True
		)

		insert_facebook_data_in_silver_marketing_cost_log = SQLExecuteQueryOperator(
			task_id=f'insert_facebook_data_in_silver_marketing_cost_log',
			sql=f"""
					SET QUERY_GROUP TO 'slow-queue';
					INSERT INTO silver.marketing_cost_log (
						event_time, country_code, media_source, advertiser_id, advertiser_name, campaign_id, 
						campaign_name, adset_id, adset_name, ad_id, ad_name, ad_short_name, impressions, 
						clicks, spend, currency, video_watched_25, video_watched_50, video_watched_75, 
						video_watched_100, video_watched_2s, avg_play_time, app_name, platform
					)
					SELECT
						CAST(date_stop AS VARCHAR) AS event_time,
						CAST(country AS VARCHAR) AS country_code,
						CAST(media_source AS VARCHAR) AS media_source,
						CAST(account_id AS VARCHAR) AS advertiser_id,
						CAST(account_name AS VARCHAR) AS advertiser_name,
						CAST(campaign_id AS VARCHAR) AS campaign_id,
						CAST(campaign_name AS VARCHAR) AS campaign_name,
						CAST(adset_id AS VARCHAR) AS adset_id,
						CAST(adset_name AS VARCHAR) AS adset_name,
						CAST(ad_id AS VARCHAR) AS ad_id,
						CAST(ad_name AS VARCHAR) AS ad_name,
						NULL AS ad_short_name,
						CAST(impressions AS VARCHAR) AS impressions,
						CAST(clicks AS VARCHAR) AS clicks,
						CAST(spend AS VARCHAR) AS spend,
						CAST(account_currency AS VARCHAR) AS currency,
						CAST(video_p25_watched_actions AS VARCHAR) AS video_watched_25,
						CAST(video_p50_watched_actions AS VARCHAR) AS video_watched_50,
						CAST(video_p75_watched_actions AS VARCHAR) AS video_watched_75,
						CAST(video_p100_watched_actions AS VARCHAR) AS video_watched_100,
						NULL AS video_watched_2s,
						NULL AS avg_play_time,
						NULL AS app_name,
						NULL AS platform
					FROM bronze.facebook_ad_spend
					WHERE event_time::DATE >= '{variables["facebook_time_delta"] if variables["facebook_time_delta"] != 'AUTO' else str(current_date - timedelta(days=15))}';
				""",
			split_statements=True,
			conn_id='aws_redshift_cluster',
			autocommit=True
		)

		insert_tiktok_data_in_silver_marketing_cost_log = SQLExecuteQueryOperator(
			task_id=f'insert_tiktok_data_in_silver_marketing_cost_log',
			sql=f"""
							SET QUERY_GROUP TO 'slow-queue';
							INSERT INTO silver.marketing_cost_log (
								event_time, country_code, media_source, advertiser_id, advertiser_name, campaign_id, 
								campaign_name, adset_id, adset_name, ad_id, ad_name, ad_short_name, impressions, 
								clicks, spend, currency, video_watched_25, video_watched_50, video_watched_75, 
								video_watched_100, video_watched_2s, avg_play_time, app_name, platform
							)
							SELECT
								CAST(ad_spend_time AS VARCHAR) AS event_time,
								CAST(country_code AS VARCHAR) AS country_code,
								CAST(media_source AS VARCHAR) AS media_source,
								CAST(advertiser_id AS VARCHAR) AS advertiser_id,
								CAST(advertiser_name AS VARCHAR) AS advertiser_name,
								CAST(campaign_id AS VARCHAR) AS campaign_id,
								CAST(campaign_name AS VARCHAR) AS campaign_name,
								CAST(adset_id AS VARCHAR) AS adset_id,
								CAST(adset_name AS VARCHAR) AS adset_name,
								CAST(ad_id AS VARCHAR) AS ad_id,
								CAST(ad_name AS VARCHAR) AS ad_name,
								NULL AS ad_short_name,
								CAST(impressions AS VARCHAR) AS impressions,
								CAST(clicks AS VARCHAR) AS clicks,
								CAST(spend AS VARCHAR) AS spend,
								CAST(currency AS VARCHAR) AS currency,
								CAST(video_p25_watched_actions AS VARCHAR) AS video_watched_25,
								CAST(video_p50_watched_actions AS VARCHAR) AS video_watched_50,
								CAST(video_p75_watched_actions AS VARCHAR) AS video_watched_75,
								CAST(video_p100_watched_actions AS VARCHAR) AS video_watched_100,
								CAST(video_watched_2s AS VARCHAR) AS video_watched_2s,
								CAST(average_video_play AS VARCHAR) AS avg_play_time,
								NULL AS app_name,
								NULL AS platform
							FROM bronze.tiktok_ads_manager_spend_event
							WHERE event_time::DATE >= '{variables["tiktok_time_delta"] if variables["tiktok_time_delta"] != 'AUTO' else str(current_date - timedelta(days=15))}';
						""",
			split_statements=True,
			conn_id='aws_redshift_cluster',
			autocommit=True
		)

		create_table_silver_marketing_cost_log >> \
		[insert_facebook_data_in_silver_marketing_cost_log, insert_tiktok_data_in_silver_marketing_cost_log]

	with TaskGroup(group_id="create_gold_dim_ad_group") as create_gold_dim_ad_group:
		create_silver_marketing_unique_ads = SQLExecuteQueryOperator(
			task_id=f'create_silver_marketing_unique_ads',
			sql=f"""
				SET query_group TO 'slow-queue';
                DROP TABLE IF EXISTS silver.marketing_unique_ads;
                CREATE TABLE silver.marketing_unique_ads AS
				WITH preprocessed_ads AS (
					SELECT
						CASE
							WHEN LOWER(COALESCE(ad_id, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE ad_id
						END AS ad_id,
						CASE
							WHEN LOWER(COALESCE(ad_short_name, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE ad_short_name
						END AS ad_short_name,
						CASE
							WHEN LOWER(COALESCE(ad_name, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE ad_name
						END AS ad_name,
						CASE
							WHEN (
								LOWER(COALESCE(ad_name, 'none')) LIKE '%.mp4%' OR
								LOWER(COALESCE(ad_name, 'none')) LIKE '%.mov%' OR
								LOWER(COALESCE(ad_name, 'none')) LIKE '%video%'
							) THEN 'Video'
							ELSE 'Static'
						END AS ad_media_type,
						CASE
							WHEN LOWER(COALESCE(ad_id, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE
								CASE
									WHEN LOWER(COALESCE(adset_id, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
									ELSE adset_id
								END
						END AS adset_id,
						CASE
							WHEN LOWER(COALESCE(ad_id, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE
								CASE
									WHEN LOWER(COALESCE(adset_name, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
									ELSE adset_name
								END
						END AS adset_name,
						CASE
							WHEN LOWER(COALESCE(campaign_id, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE campaign_id
						END AS campaign_id,
						CASE
							WHEN LOWER(COALESCE(campaign_name, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE campaign_name
						END AS campaign_name,
						CASE
							WHEN LOWER(COALESCE(advertiser_id, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE advertiser_id
						END AS advertiser_id,
						CASE
							WHEN LOWER(COALESCE(advertiser_name, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE advertiser_name
						END AS advertiser_name,
						media_source,
						CASE
							WHEN LOWER(COALESCE(app_name, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE app_name
						END AS app_name,
						CASE
							WHEN LOWER(COALESCE(platform, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE platform
						END AS platform,
						MAX(event_time::date) AS event_time
					FROM silver.marketing_cost_log
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
				),
                ads_with_calculated_fingerprints AS (
                   	SELECT
                        FARMFINGERPRINT64(COALESCE(PA.ad_id, 'NULL') || COALESCE(PA.campaign_id, 'NULL')) AS ad_sk,
                        ad_id,
                        ad_short_name,
                        ad_name,
                        ad_media_type,
                        adset_id,
                        adset_name,
                        campaign_id,
                        campaign_name,
                        advertiser_id,
                        advertiser_name,
						CASE
							WHEN lower(campaign_name) LIKE '%dlo%' THEN 'dlo'
							WHEN regexp_instr(lower(campaign_name), '(spanish|spain|\_es\_)') > 0 THEN 'es'
							WHEN regexp_instr(lower(campaign_name), '(french|\_fr\_)') > 0 THEN 'fr'
							WHEN regexp_instr(lower(campaign_name), '(german|\_de\_)') > 0 THEN 'de'
							WHEN regexp_instr(lower(campaign_name), '(turkiye)') > 0 THEN 'tr'
							WHEN regexp_instr(lower(campaign_name), '(polish)') > 0 THEN 'pl'
							WHEN regexp_instr(lower(campaign_name), '(japanese)') > 0 THEN 'jp'
							WHEN regexp_instr(lower(campaign_name), '(?<!@)[^@]+(?=_l@)', 1, 1, 0, 'p') > 0 THEN regexp_substr(lower(campaign_name), '(?<!@)[^@]+(?=_l@)', 1, 1, 'p')
							ELSE 'en'
						END AS locale,
						CASE
							WHEN regexp_instr(lower(campaign_name), '(?<!@)[^@]+(?=_f@)', 1, 1, 0, 'p') > 0 THEN regexp_substr(lower(campaign_name), '(?<!@)[^@]+(?=_f@)', 1, 1, 'p')
							WHEN regexp_instr(lower(campaign_name), '([^a-zA-Z0-9]palm?(istry)?)|(^pal)') > 0 THEN 'palmistry'
							when regexp_instr(lower(campaign_name), 'human design') > 0 then 'human-design'
							when regexp_instr(lower(campaign_name), 'malearch') > 0 then 'male-archetypes'
							when regexp_instr(lower(campaign_name), 'mystic-power') > 0 then 'mystic-power'
							when regexp_instr(lower(campaign_name), 'feminine') > 0 then 'feminine-archetypes'
							when regexp_instr(lower(campaign_name), 'hypno-healing') > 0 then 'hypno-healing'
							when regexp_instr(lower(campaign_name), 'hypno-healing-thinking') > 0 then 'hypno-healing-thinking' 
							when regexp_instr(lower(campaign_name), 'lonarch') > 0 then 'loneliness-archetypes' 
							when regexp_instr(lower(campaign_name), 'soul-connection') > 0 then 'soul-connection'
							WHEN regexp_instr(lower(campaign_name), 'mooncomp') > 0 THEN 'moon-compatibility'
							WHEN regexp_instr(lower(campaign_name), 'natal') > 0 THEN 'natal-chart'
							WHEN regexp_instr(lower(campaign_name), 'lovecomp') > 0 THEN 'love-compatibility'
							WHEN regexp_instr(lower(campaign_name), 'witch\s?power') > 0 THEN 'witch-power'
							WHEN regexp_instr(lower(campaign_name), 'femarch') > 0 THEN 'feminine-archetypes'
							WHEN regexp_instr(lower(campaign_name), 'app-sub-intro') > 0 THEN 'app-sub-intro'
							WHEN regexp_instr(lower(campaign_name), 'human[_ ]design') > 0 THEN 'human-design'
							WHEN regexp_instr(lower(campaign_name), 'sub_shamans') > 0 THEN 'shamans'
							WHEN regexp_instr(lower(campaign_name), 'intropalm') > 0 THEN 'palmistry-intro'
							WHEN regexp_instr(lower(campaign_name), 'attachment-style') > 0 THEN 'attachment-style'
							WHEN regexp_instr(lower(campaign_name), 'attachment-zodiac') > 0 THEN 'attachment-style-zodiac'
							WHEN regexp_instr(lower(campaign_name), 'spiritanimal') > 0 THEN 'spirit-animal'
							WHEN regexp_instr(lower(campaign_name), 'intro_witch') > 0 THEN 'witch-power-intro'
							WHEN regexp_instr(lower(campaign_name), 'sub-karma') > 0 THEN 'karma'
							WHEN regexp_instr(lower(campaign_name), 'love-addiction') > 0 THEN 'love-addiction'
							WHEN regexp_instr(lower(campaign_name), 'hypno-intro') > 0 THEN 'hypnotherapy-intro'
							WHEN regexp_instr(lower(campaign_name), 'sub-goddess') > 0 THEN 'goddess'
							WHEN regexp_instr(lower(campaign_name), 'star-seed') > 0 THEN 'star-seed'
							WHEN regexp_instr(lower(campaign_name), 'relationship-anxiety') > 0 THEN 'relationship-anxiety'
							WHEN regexp_instr(lower(campaign_name), 'life-control') > 0 THEN 'life-control'
							WHEN regexp_instr(lower(campaign_name), 'codependency') > 0 THEN 'codependency'
							WHEN regexp_instr(lower(campaign_name), 'lonely') > 0 THEN 'loneliness'
							WHEN lower(campaign_name) like '%simple_simple_registration%' then 'simple_simple_registration'
							WHEN lower(campaign_name) like '%simple_reg%' then 'simple_registration'
							WHEN lower(campaign_name) like '%catalogue%' then 'experts_catalogue'
							WHEN lower(campaign_name) like '%psychic%' then 'chat_bot'
							WHEN lower(campaign_name) like '%simple_sub%' then 'simple_subscription'
							WHEN lower(campaign_name) like '%cheating%' then 'simple_registration_cheating'
							when campaign_name LIKE '%HBT-N%' THEN 'hypno-healing-thinking'
							ELSE NULL
						END AS funnel,
						CASE 
							WHEN funnel IN (
								'cheating',
								'simple_reg',
								'simple_registration', 
								'experts_catalogue', 
								'chat_bot', 
								'simple_subscription', 
								'simple_registration_cheating',
								'simple_simple_registration'
							) THEN 'web_asknebula'
							WHEN advertiser_name IN (
								'SIA DIGIBRAND - Nebula AND2 - Httpool USD',
								'SIA DIGIBRAND - Nebula AND3 - Httpool USD',
								'SIA DIGIBRAND - Nebula AND4 - Httpool USD', 
								'SIA DIGIBRAND - Nebula CAPI Android 2 - Httpool USD',
								'Nebula Android GMT'
							) THEN 'mobile_android'
							WHEN advertiser_name IN (
								'SIA DIGIBRAND - Nebula iOS14 2 - Httpool USD',
								'Nebula iOS CAPI test',
								'Nebula iOS 12 - iOS 14 test',
								'Maze',
								'ironsource_int',
								'IOS_web',
								'Apple Search Ads',
								'Adsbalance'
								'Nebula iOS 14',
								'Nebula WEB 4 CAPI Nebula iOS',
								 'SIA DIGIBRAND - Nebula CAPI - Httpool USD'
							) THEN 'mobile_ios'
							WHEN advertiser_name IN ('Nebula SMM', 'Obrio SMM') THEN NULL
							ELSE 'web_appnebula'
						END AS funnel_target_unit,
						media_source,
						CASE
							WHEN LOWER(COALESCE(app_name, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE app_name
						END AS app_name,
						CASE
							WHEN LOWER(COALESCE(platform, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE platform
						END AS platform,
						CASE
        					WHEN regexp_instr(lower(campaign_name), '(?<!@)[^@]+(?=_m@)', 1, 1, 0, 'p') > 0 THEN regexp_substr(lower(campaign_name), '(?<!@)[^@]+(?=_m@)', 1, 1, 'p')
    					END as manager,
						CASE
        					WHEN regexp_instr(lower(campaign_name), '(?<!@)[^@]+(?=_bo@)', 1, 1, 0, 'p') > 0 THEN regexp_substr(lower(campaign_name), '(?<!@)[^@]+(?=_bo@)', 1, 1, 'p')
   						END as budget_optimisation,
        				CASE
        					WHEN regexp_instr(lower(campaign_name), '(?<!@)[^@]+(?=_pg@)', 1, 1, 0, 'p') > 0 THEN regexp_substr(lower(campaign_name), '(?<!@)[^@]+(?=_pg@)', 1, 1, 'p')
    					END as campaign_goal,
                        FARMFINGERPRINT64(
                            COALESCE(ad_id, 'NULL') ||
                            COALESCE(ad_short_name, 'NULL') ||
                            COALESCE(ad_name, 'NULL') ||
                            COALESCE(ad_media_type, 'NULL') ||
                            COALESCE(adset_id, 'NULL') ||
                            COALESCE(adset_name, 'NULL') ||
                            COALESCE(campaign_id, 'NULL') ||
                            COALESCE(campaign_name, 'NULL') ||
                            COALESCE(advertiser_id, 'NULL') ||
                            COALESCE(advertiser_name, 'NULL') ||
                            COALESCE(locale, 'NULL') ||
                            COALESCE(funnel, 'NULL') ||
                            COALESCE(funnel_target_unit, 'NULL') ||
                            COALESCE(media_source, 'NULL') ||
                            COALESCE(app_name, 'NULL') ||
                            COALESCE(platform, 'NULL') ||
                            COALESCE(manager, 'NULL') ||
                            COALESCE(budget_optimisation, 'NULL') ||
                            COALESCE(campaign_goal, 'NULL')
                        ) AS fingerprint,
                        event_time
                    FROM preprocessed_ads AS PA
                )
				SELECT
                    *
                FROM ads_with_calculated_fingerprints
				WHERE ad_id IS NOT NULL OR campaign_id IS NOT NULL
                QUALIFY ROW_NUMBER() OVER(PARTITION BY ad_sk ORDER BY event_time DESC) = 1;
			""",
			split_statements=True,
			conn_id='aws_redshift_cluster',
			autocommit=True
		)

		insert_new_gold_dim_ad_scd = SQLExecuteQueryOperator(
			task_id=f'insert_new_gold_dim_ad_scd',
			sql=f'''
				SET query_group TO 'fast-queue';
				INSERT INTO gold.dim_ad
				SELECT 
					ad_sk, 
					ad_id,
					ad_short_name, 
					ad_name,
					ad_media_type, 
					adset_id,
					adset_name,
					campaign_id, 
					campaign_name, 
					advertiser_id, 
					advertiser_name, 
					locale, 
					funnel, 
					funnel_target_unit,
					media_source, 
					app_name, 
					platform,
					manager,
					budget_optimisation,
					campaign_goal,
					fingerprint,
					CURRENT_TIMESTAMP AS valid_from,
					CAST('9999-01-01 00:00:00' AS DATETIME) AS valid_to,
					1 AS is_active 
				FROM silver.marketing_unique_ads AS orgn
				WHERE orgn.ad_sk NOT IN (SELECT DISTINCT ad_sk FROM gold.dim_ad);
			''',
			split_statements=True,
			conn_id='aws_redshift_cluster',
			autocommit=True
		)

		insert_modified_gold_dim_ad_scd = SQLExecuteQueryOperator(
			task_id=f'insert_modified_gold_dim_ad_scd',
			sql=f'''
				SET query_group TO 'fast-queue';
				INSERT INTO gold.dim_ad
				SELECT 
					orgn.ad_sk, 
					orgn.ad_id, 
					orgn.ad_short_name, 
					orgn.ad_name, 
					orgn.ad_media_type,
					orgn.adset_id,
					orgn.adset_name, 
					orgn.campaign_id, 
					orgn.campaign_name, 
					orgn.advertiser_id, 
					orgn.advertiser_name, 
					orgn.locale, 
					orgn.funnel,
					orgn.funnel_target_unit, 
					orgn.media_source, 
					orgn.app_name, 
					orgn.platform,
					orgn.manager,
					orgn.budget_optimisation,
					orgn.campaign_goal,    
					orgn.fingerprint,
					CURRENT_TIMESTAMP AS valid_from,
					CAST('9999-01-01 00:00:00' AS DATETIME) AS valid_to,
					1 AS is_active 
				FROM silver.marketing_unique_ads AS orgn
				INNER JOIN gold.dim_ad AS tgt
					ON orgn.ad_sk = tgt.ad_sk AND orgn.fingerprint <> tgt.fingerprint AND tgt.is_active = TRUE;
			''',
			split_statements=True,
			conn_id='aws_redshift_cluster',
			autocommit=True
		)

		update_old_gold_dim_ad_scd = SQLExecuteQueryOperator(
			task_id=f'update_old_gold_dim_ad_scd',
			sql=f'''
				SET query_group TO 'fast-queue';
				UPDATE gold.dim_ad AS tgt
				SET 
					is_active = FALSE,
					valid_to = orgn.valid_from - INTERVAL '1 SECOND'
				FROM (
					SELECT ad_sk, valid_from, fingerprint FROM gold.dim_ad
				) AS orgn
				WHERE orgn.ad_sk = tgt.ad_sk AND 
					orgn.fingerprint <> tgt.fingerprint AND 
					orgn.valid_from > tgt.valid_from AND
					tgt.is_active = TRUE;
			''',
			split_statements=True,
			conn_id='aws_redshift_cluster',
			autocommit=True
		)

		create_silver_marketing_unique_ads >> \
		insert_new_gold_dim_ad_scd >> \
		insert_modified_gold_dim_ad_scd >> \
		update_old_gold_dim_ad_scd

	with TaskGroup(group_id="create_gold_fact_ad_spend_group") as create_gold_fact_ad_spend_group:

		create_silver_marketing_ad_spend_enriched = SQLExecuteQueryOperator(
			task_id='create_silver_marketing_ad_spend_enriched',
			sql = f"""
				SET query_group TO 'slow-queue';
				DROP TABLE IF EXISTS silver.marketing_ad_spend_enriched;
				CREATE TABLE silver.marketing_ad_spend_enriched AS
				WITH all_preprocessed_ads AS (
					SELECT
						CASE
							WHEN LOWER(COALESCE(ad_id, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE ad_id
						END AS ad_id,
						CASE
							WHEN LOWER(COALESCE(adset_id, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE adset_id
						END AS adset_id,
						CASE
							WHEN LOWER(COALESCE(campaign_id, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE campaign_id
						END AS campaign_id,
						CASE
							WHEN LOWER(COALESCE(advertiser_id, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE advertiser_id
						END AS advertiser_id,
						media_source,
						CAST(event_time AS TIMESTAMP) AS ad_spend_time,
						CASE
							WHEN LOWER(COALESCE(country_code, 'none')) IN ('none', 'unknown', 'nan') THEN NULL
							ELSE country_code
						END AS country_iso_code,
						currency AS currency_iso_code,
						CAST(impressions AS INT) AS impression_qty,
						CAST(clicks AS INT) AS click_qty,
						CAST(spend AS NUMERIC(18, 10)) AS spend_amt,
						CAST(video_watched_25 AS INT) AS video_time_25_pct_qty,
						CAST(video_watched_50 AS INT) AS video_time_50_pct_qty,
						CAST(video_watched_75 AS INT) AS video_time_75_pct_qty,
						CAST(video_watched_100 AS INT) AS video_time_100_pct_qty,
						CAST(video_watched_2s AS INT) AS video_time_2s_qty,
						CAST(avg_play_time AS NUMERIC(18, 10)) AS video_time_avg_qty
					FROM silver.marketing_cost_log
				),
				preprocessed_full_ads_sources AS (
					SELECT
						FARMFINGERPRINT64(
							CAST(FARMFINGERPRINT64(COALESCE(ad_id, 'NULL') || COALESCE(campaign_id, 'NULL')) AS VARCHAR) ||
							COALESCE(CAST(ad_spend_time AS VARCHAR), 'NULL') ||
							COALESCE(country_iso_code, 'NULL')
						) AS ad_spend_sk,
						FARMFINGERPRINT64(COALESCE(ad_id, 'NULL') || COALESCE(campaign_id, 'NULL')) AS ad_sk,
						ad_id,
						campaign_id,
						media_source,
						ad_spend_time,
						country_iso_code,
						currency_iso_code,
						impression_qty,
						click_qty,
						spend_amt,
						video_time_25_pct_qty,
						video_time_50_pct_qty,
						video_time_75_pct_qty,
						video_time_100_pct_qty,
						video_time_2s_qty,
						video_time_avg_qty
					FROM all_preprocessed_ads
					WHERE ad_id IS NOT NULL AND campaign_id IS NOT NULL
				),
				preprocessed_campaign_only_sources AS (
					SELECT
						FARMFINGERPRINT64(
							CAST(FARMFINGERPRINT64(COALESCE(ad_id, 'NULL') || COALESCE(campaign_id, 'NULL')) AS VARCHAR) ||
							COALESCE(CAST(ad_spend_time AS VARCHAR), 'NULL') ||
							COALESCE(country_iso_code, 'NULL')
						) AS ad_spend_sk,
						FARMFINGERPRINT64(COALESCE(ad_id, 'NULL') || COALESCE(campaign_id, 'NULL')) AS ad_sk,
						ad_id,
						campaign_id,
						media_source,
						ad_spend_time,
						country_iso_code,
						currency_iso_code,
						SUM(impression_qty) as impression_qty,
						SUM(click_qty) as click_qty,
						SUM(spend_amt) as spend_amt,
						CAST(NULL AS INT) AS video_time_25_pct_qty,
						CAST(NULL AS INT) AS video_time_50_pct_qty,
						CAST(NULL AS INT) AS video_time_75_pct_qty,
						CAST(NULL AS INT) AS video_time_100_pct_qty,
						CAST(NULL AS INT) AS video_time_2s_qty,
						CAST(NULL AS INT) AS video_time_avg_qty
					FROM all_preprocessed_ads AS orgn
					WHERE ad_id IS NULL AND campaign_id IS NOT NULL
					GROUP BY 1,2,3,4,5,6,7,8
				),
				unified_ads_and_campaigns AS (
					SELECT * FROM preprocessed_full_ads_sources
					UNION ALL
					SELECT * FROM preprocessed_campaign_only_sources
				)
				SELECT
					ad_spend_sk,
					ad_sk,
					ad_id,
					campaign_id,
					ad_spend_time,
					country_iso_code,
					currency_iso_code,
					impression_qty,
					click_qty,
					spend_amt,
					video_time_25_pct_qty,
					video_time_50_pct_qty,
					video_time_75_pct_qty,
					video_time_100_pct_qty,
					video_time_2s_qty,
					video_time_avg_qty,
					CURRENT_TIMESTAMP AS insertion_time
				FROM unified_ads_and_campaigns
				WHERE 1=1
				QUALIFY ROW_NUMBER() OVER(PARTITION BY ad_spend_sk) = 1
				;
			""",
			split_statements = True,
			conn_id = 'aws_redshift_cluster',
			autocommit = True
		)

		delete_repeated_gold_fact_ad_spend_data = SQLExecuteQueryOperator(
			task_id = 'delete_repeated_gold_fact_ad_spend_data',
			sql = f"""
				SET query_group TO 'fast-queue';
				DELETE FROM gold.fact_ad_spend
				USING silver.marketing_ad_spend_enriched
				WHERE 
					fact_ad_spend.ad_spend_sk = marketing_ad_spend_enriched.ad_spend_sk;
			""",
			split_statements = True,
			conn_id = 'aws_redshift_cluster',
			autocommit = True
		)

		insert_gold_fact_ad_spend_data = SQLExecuteQueryOperator(
			task_id = 'insert_gold_fact_ad_spend_data',
			sql = f"""
				SET query_group TO 'fast-queue';
				INSERT INTO gold.fact_ad_spend 
				SELECT * FROM silver.marketing_ad_spend_enriched;
			""",
			split_statements = True,
			conn_id = 'aws_redshift_cluster',
			autocommit = True
		)

		create_silver_marketing_ad_spend_enriched >> \
		delete_repeated_gold_fact_ad_spend_data >> \
		insert_gold_fact_ad_spend_data

	end_operator = EmptyOperator(task_id="dag_end", outlets=[Dataset(f"s3://prod-vmart-redshift-analytics/dataset/{dag_id}.txt")])

	start_operator >> \
	create_silver_marketing_cost_log >> \
	[create_gold_dim_ad_group, create_gold_fact_ad_spend_group] >> \
	end_operator