import json
import os
from datetime import date, datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

dag_id = "dag_gold_marketing_order"
current_date = date.today()
json_variables_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"variables/{dag_id}.json")

with open(json_variables_path, 'r') as j:
    variables = json.loads(j.read())

with DAG(
        dag_id=dag_id,
        start_date=datetime(2025, 3, 6),
        schedule=[Dataset("s3://prod-vmart-redshift-analytics/dataset/dag_bronze_solid_webhook_events.txt"),
                  Dataset("s3://prod-vmart-redshift-analytics/dataset/dag_bronze_webhook_app_orders.txt")],
        max_active_runs=1,
        description='DAG for performing marketing attribution',
        tags=['prod', 'gold', 'marketing', 'orders'],
        default_args={
            'owner': 'airflow',
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
        },
        catchup=False
) as dag:
    start_operator = EmptyOperator(task_id="dag_start")

    create_silver_solid_webhook_subscription_order = SQLExecuteQueryOperator(
        task_id=f'create_silver_solid_webhook_subscription_order',
        sql=f"""
			SET query_group TO 'slow-queue';
			DROP TABLE IF EXISTS silver.solid_webhook_subscription_order;
			CREATE TABLE silver.solid_webhook_subscription_order AS
			(
			WITH preprocessed_solid_subscription AS (
			SELECT
                    event_id,
                    event_time,
                    event_created_at,
                    JSON_EXTRACT_PATH_TEXT(subscription, 'id', true) AS subscription_id,
                    JSON_EXTRACT_PATH_TEXT(subscription, 'payment_type', true) AS order_payment_method,
                    JSON_EXTRACT_PATH_TEXT(product, 'name', true) AS product_name,
                    JSON_EXTRACT_PATH_TEXT(product, 'currency', true) AS order_original_currency,
                    JSON_EXTRACT_PATH_TEXT(customer, 'customer_account_id', true) AS user_id,
                    JSON_PARSE(invoices) AS invoices
                FROM bronze.solid_webhook_subscription_order
                WHERE event_created_at::date >= DATEADD(day, -3, current_date)
            ),
            subscription_to_invoice_id_mapping AS (
                SELECT DISTINCT
                    subscription_id,
                    attr AS invoice_id
                FROM preprocessed_solid_subscription AS a,
                UNPIVOT a.invoices AS val AT attr
            ),
            preprocessed_solid_invoices AS (
                SELECT
                    a.subscription_id,
                    a.order_payment_method,
                    a.product_name,
                    a.order_original_currency,
                    a.user_id,
                    b.invoice_id,
                    JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(a.invoices), b.invoice_id, true) AS invoice,
                    JSON_PARSE(JSON_EXTRACT_PATH_TEXT(invoice, 'orders', true)) AS orders
                FROM preprocessed_solid_subscription AS a
                INNER JOIN subscription_to_invoice_id_mapping AS b
                    ON a.subscription_id = b.subscription_id
                WHERE invoice IS NOT NULL
            ),
            subscription_to_order_id_mapping AS (
                SELECT DISTINCT
                    a.subscription_id,
                    a.invoice_id,
                    attr AS order_id
                FROM preprocessed_solid_invoices AS a,
                UNPIVOT a.orders AS val AT attr
            ),
            preprocessed_solid_orders AS (
                SELECT
                    JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(a.orders), b.order_id, true) AS "order",
                    a.subscription_id,
                    a.invoice_id,
                    a.invoice,
                    a.order_payment_method,
                    a.product_name,
                    a.order_original_currency,
                    a.user_id,
                    'USD' AS order_converted_currency,
                    JSON_EXTRACT_PATH_TEXT("order", 'id', true) AS order_id,
                    JSON_EXTRACT_PATH_TEXT("order", 'status', true) AS order_status,
                    CAST(JSON_EXTRACT_PATH_TEXT("order", 'created_at', true) AS TIMESTAMP) AS order_created_at,
                    CAST(JSON_EXTRACT_PATH_TEXT("order", 'amount', true) AS NUMERIC(18, 10)) / 100 AS order_original_amt
                FROM preprocessed_solid_invoices AS a
                INNER JOIN subscription_to_order_id_mapping AS b
                    ON a.subscription_id = b.subscription_id AND a.invoice_id = b.invoice_id
                WHERE "order" IS NOT NULL
            ),
            
            processed_deduplicated_orders AS (
                SELECT 
                    order_id,
                    CASE
                        WHEN order_status IN ('refunded', 'void_ok')
                            THEN 'refunded'
                        WHEN order_status IN ('auth_ok', 'settle_ok', 'partial_settled', 'approved')
                            THEN 'approved'
                    END AS order_status,
                    order_created_at,
                    order_original_amt,
                    subscription_id,
                    order_payment_method,
                    product_name,
                    order_original_currency,
                    user_id,
                    order_converted_currency
                FROM preprocessed_solid_orders
                WHERE order_status IN ('auth_ok', 'settle_ok', 'partial_settled', 'approved', 'refunded', 'void_ok')
                QUALIFY ROW_NUMBER() OVER(PARTITION BY order_id, order_status ORDER BY order_created_at::timestamp DESC) = 1
            ),
            final_subscription_order AS (
                SELECT 
                    FARMFINGERPRINT64(COALESCE(A.order_id, 'NULL') || COALESCE(A.order_status, 'NULL')) AS order_sk,
                    A.order_id,
                    A.order_status,
                    A.order_created_at,
                    CASE 
                        WHEN A.order_original_currency = 'USD' 
                            THEN ROUND(A.order_original_amt, 2)
                        WHEN A.order_original_currency = 'EUR' 
                            THEN ROUND(A.order_original_amt / COALESCE(E.usd_to_eur, (SELECT usd_to_eur FROM reference.exchange_rate WHERE "date" = (SELECT MAX("date") FROM reference.exchange_rate))), 2)
                        WHEN A.order_original_currency = 'MXN' 
                            THEN ROUND(A.order_original_amt / COALESCE(E.usd_to_mxn, (SELECT usd_to_mxn FROM reference.exchange_rate WHERE "date" = (SELECT MAX("date") FROM reference.exchange_rate))), 2)  
                        WHEN A.order_original_currency = 'GBP' 
                            THEN ROUND(A.order_original_amt / COALESCE(E.usd_to_gbp, (SELECT usd_to_gbp FROM reference.exchange_rate WHERE "date" = (SELECT MAX("date") FROM reference.exchange_rate))), 2) 
                    END AS order_converted_amt,
                    A.order_converted_currency,
                    A.order_original_amt,
                    A.order_original_currency, 
                    A.order_payment_method,
                    A.product_name,
                    CASE 
                        WHEN D.product_type IS NOT NULL THEN D.product_type
                        ELSE NULL
                    END AS product_type,
                    A.user_id,
                    A.subscription_id
                FROM processed_deduplicated_orders AS A
                LEFT JOIN reference.product_mapping AS D
                    ON A.product_name = D.product_name
                LEFT JOIN reference.exchange_rate AS E 
                    ON A.order_created_at = E."date"
            ),
            silver_subscription_order AS (
                SELECT 
                    A.order_sk,
                    A.order_id,
                    A.order_status,
                    A.order_created_at,
                    CAST(NULL AS VARCHAR) AS order_country,
                    A.order_converted_amt,
                    A.order_converted_currency,
                    A.order_original_amt,
                    A.order_original_currency,
                    A.order_payment_method,
                    CAST(NULL AS VARCHAR) AS order_platform,
                    A.product_name,
                    A.product_type,
                    CASE 
                        WHEN C.id IS NOT NULL THEN CAST(C.nebula_id AS VARCHAR)
                        ELSE A.user_id
                    END AS user_id,
                    A.subscription_id,
                    CAST(NULL AS INT) AS product_renewal_number
                FROM final_subscription_order A
                LEFT JOIN reference.user_id_mapping C
                    ON A.user_id = C.id
                QUALIFY ROW_NUMBER() OVER(PARTITION BY order_sk ORDER BY order_created_at DESC) = 1
            )
            
            SELECT 
                A.* 
            FROM silver_subscription_order as A
            );
			""",
        split_statements=True,
        conn_id='aws_redshift_cluster',
        autocommit=True
    )

    insert_gold_solid_subscription_order = SQLExecuteQueryOperator(
        task_id=f'insert_gold_solid_subscription_order',
        sql=f"""
				SET query_group TO 'slow-queue';
				INSERT INTO gold.fact_subscription_order
					SELECT origin.* FROM silver.solid_webhook_subscription_order as origin
				WHERE origin.order_sk NOT IN (SELECT DISTINCT order_sk FROM gold.fact_subscription_order);
			""",
        split_statements=True,
        conn_id='aws_redshift_cluster',
        autocommit=True
    )

    create_silver_solid_webhook_card_order = SQLExecuteQueryOperator(
        task_id=f'create_silver_solid_webhook_card_order',
        sql=f"""
			SET query_group TO 'slow-queue';
			DROP TABLE IF EXISTS silver.solid_webhook_card_order;
			CREATE TABLE silver.solid_webhook_card_order AS
				WITH order_with_parsed_json AS (
					SELECT
						JSON_EXTRACT_PATH_TEXT("order", 'order_id', true)::varchar AS order_id,
						CASE
							WHEN JSON_EXTRACT_PATH_TEXT("order", 'status', true) IN ('auth_ok') THEN 'approved'::varchar
							WHEN JSON_EXTRACT_PATH_TEXT("order", 'status', true) IN ('void_ok', 'refunded') THEN 'refunded'::varchar
						END AS order_status,
						CASE
							WHEN order_status = 'approved' THEN JSON_EXTRACT_PATH_TEXT("transaction", 'created_at', true)::timestamp
							WHEN order_status = 'refunded' THEN JSON_EXTRACT_PATH_TEXT("transaction", 'updated_at', true)::timestamp
						END AS order_created_at,
						CAST(NULL AS DOUBLE PRECISION) AS order_converted_amt,
						'USD' AS order_converted_currency,
						CAST(JSON_EXTRACT_PATH_TEXT("order", 'amount', true) AS NUMERIC(18, 10)) / 100 AS order_original_amt,
						JSON_EXTRACT_PATH_TEXT("order", 'currency', true) AS order_original_currency,
						'card' AS order_payment_method,
						JSON_EXTRACT_PATH_TEXT("order_metadata", 'product', true) AS product_name,
						CAST(NULL AS VARCHAR) AS product_type,
						JSON_EXTRACT_PATH_TEXT("order", 'customer_email', true) AS user_email,
						JSON_EXTRACT_PATH_TEXT("order_metadata", 'user_id', true) AS user_id,
						JSON_EXTRACT_PATH_TEXT("order", 'subscription_id', true) AS subscription_id,
						JSON_PARSE(transactions) AS transactions,
						JSON_EXTRACT_PATH_TEXT("order_metadata", 'project', true) AS order_platform
					FROM bronze.solid_webhook_card_order
					WHERE (
					        JSON_EXTRACT_PATH_TEXT("order", 'status', true) IN ('auth_ok') 
					            OR
                            (
                                JSON_EXTRACT_PATH_TEXT("order", 'status', true) IN ('void_ok', 'refunded') 
                                    AND 
                                JSON_EXTRACT_PATH_TEXT("transaction", 'operation', true) IN ('void', 'refund')
                                    AND 
                                JSON_EXTRACT_PATH_TEXT("transaction", 'status', true) = 'success'
                            )
                        )
						AND 
						    event_created_at::date >= DATEADD(day, -{variables["webhook_card_time_delta"]}, current_date)
					QUALIFY ROW_NUMBER() OVER(
                        PARTITION BY order_id, order_status ORDER BY 
                            JSON_EXTRACT_PATH_TEXT("transaction", 'created_at', true)::timestamp ASC,
                            JSON_EXTRACT_PATH_TEXT("transaction", 'updated_at', true)::timestamp ASC) = 1
				),

				order_to_transaction_id_mapping AS (
					SELECT DISTINCT
						order_id,
						attr AS transaction_id
					FROM order_with_parsed_json AS a,
					UNPIVOT a.transactions AS val AT attr
				),

				order_to_transaction_country_mapping AS (
					SELECT DISTINCT
						A.order_id,
						JSON_EXTRACT_PATH_TEXT(
							JSON_EXTRACT_PATH_TEXT(
								JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(A.transactions), B.transaction_id, true),
									'card',
									true
							),
							'country',
							true
						) as order_country
					FROM order_with_parsed_json as A
					INNER JOIN order_to_transaction_id_mapping AS B
						ON A.order_id = B.order_id
					WHERE order_country IS NOT NULL
				),

				order_preprocessed AS (
				SELECT
					FARMFINGERPRINT64(COALESCE(A.order_id, 'NULL') || COALESCE(A.order_status, 'NULL')) AS order_sk,
					A.order_id,
					A.order_status,
					A.order_created_at,
					C.alpha_2_code AS order_country,
					CASE
						WHEN A.order_original_currency = 'USD'
							THEN ROUND(A.order_original_amt, 2)
						WHEN A.order_original_currency = 'EUR'
							THEN ROUND(A.order_original_amt / COALESCE(E.usd_to_eur, (SELECT usd_to_eur FROM reference.exchange_rate WHERE "date" = (SELECT MAX("date") FROM reference.exchange_rate))), 2)
						WHEN A.order_original_currency = 'MXN'
							THEN ROUND(A.order_original_amt / COALESCE(E.usd_to_mxn, (SELECT usd_to_mxn FROM reference.exchange_rate WHERE "date" = (SELECT MAX("date") FROM reference.exchange_rate))), 2)
						WHEN A.order_original_currency = 'GBP'
							THEN ROUND(A.order_original_amt / COALESCE(E.usd_to_gbp, (SELECT usd_to_gbp FROM reference.exchange_rate WHERE "date" = (SELECT MAX("date") FROM reference.exchange_rate))), 2)
					END AS order_converted_amt,
					A.order_converted_currency,
					A.order_original_amt,
					A.order_original_currency,
					A.order_payment_method,
					A.order_platform as order_platform,
					COALESCE(A.product_name, S.product_name) AS product_name,
					COALESCE(P.product_type, S.product_type) AS product_type,
					COALESCE(CAST(D.nebula_id AS VARCHAR), A.user_id, S.user_id) AS user_id,
					A.subscription_id,
					CAST(NULL AS INT) AS product_renewal_number
				FROM order_with_parsed_json AS A
				LEFT JOIN order_to_transaction_country_mapping AS B
					ON A.order_id = B.order_id
				LEFT JOIN gold.fact_subscription_order AS S
					ON A.order_id = S.order_id AND A.order_status = S.order_status
				LEFT JOIN reference.exchange_rate AS E
					ON A.order_created_at = E."date"
				LEFT JOIN reference.country_code AS C
					ON LOWER(B.order_country) = LOWER(C.alpha_3_code)
				LEFT JOIN reference.product_mapping AS P
					ON A.product_name = P.product_name
				LEFT JOIN reference.user_id_mapping AS D
					ON A.user_id = D.id
				WHERE A.order_status IS NOT NULL
				)

				SELECT
					A.*
				FROM order_preprocessed AS A
				QUALIFY ROW_NUMBER() OVER(PARTITION BY order_sk ORDER BY order_created_at DESC) = 1
			""",
        split_statements=True,
        conn_id='aws_redshift_cluster',
        autocommit=True
    )

    create_silver_solid_webhook_apm_order = SQLExecuteQueryOperator(
        task_id=f'create_silver_solid_webhook_apm_order',
        sql=f"""
			SET query_group TO 'slow-queue';
			DROP TABLE IF EXISTS silver.solid_webhook_apm_order;
			CREATE TABLE silver.solid_webhook_apm_order AS
			(
			WITH order_with_parced_json AS (
                SELECT
                    JSON_EXTRACT_PATH_TEXT("order", 'order_id', true)::varchar AS order_id,
                    JSON_EXTRACT_PATH_TEXT("order", 'status', true)::varchar AS order_status,
                    CASE
                        WHEN order_status = 'refunded' THEN JSON_EXTRACT_PATH_TEXT("order", 'updated_at', true)::timestamp
                        ELSE JSON_EXTRACT_PATH_TEXT("order", 'created_at', true)::timestamp
					END AS order_created_at,
                    COALESCE(
                        JSON_EXTRACT_PATH_TEXT(
                            JSON_EXTRACT_PATH_TEXT(
                                JSON_EXTRACT_PATH_TEXT("order", 'provider_data', true),
                                'billing_address',
                                true
                            ),
                            'country',
                            true
                        )
                    ) AS order_country,
                    CAST(NULL AS DOUBLE PRECISION) AS order_converted_amt,
                    'USD' AS order_converted_currency,
                    CAST(JSON_EXTRACT_PATH_TEXT("order", 'amount', true) AS NUMERIC(18, 10)) / 100 AS order_original_amt,
                    JSON_EXTRACT_PATH_TEXT("order", 'currency', true) AS order_original_currency,
                    'paypal' AS order_payment_method,
                    JSON_EXTRACT_PATH_TEXT("order_metadata", 'product', true) AS product_name,
                    CAST(NULL AS VARCHAR) AS product_type,
                    JSON_EXTRACT_PATH_TEXT("order", 'customer_email', true) AS user_email,
                    JSON_EXTRACT_PATH_TEXT("order_metadata", 'user_id', true) AS user_id,
                    JSON_EXTRACT_PATH_TEXT("order", 'subscription_id', true) AS subscription_id
                FROM bronze.solid_webhook_apm_order
                WHERE order_status IN ('approved', 'refunded')
                    AND event_created_at::date >= DATEADD(day, -{variables["webhook_apm_time_delta"]}, current_date)
                QUALIFY ROW_NUMBER() OVER(
                    PARTITION BY order_id, order_status ORDER BY order_created_at ASC) = 1
			),

			order_preprocessed AS (
			SELECT
				FARMFINGERPRINT64(COALESCE(A.order_id, 'NULL') || COALESCE(A.order_status, 'NULL')) AS order_sk,
				A.order_id,
				A.order_status,
				A.order_created_at,
				C.alpha_2_code AS order_country,
				CASE
					WHEN A.order_original_currency = 'USD'
						THEN ROUND(A.order_original_amt, 2)
					WHEN A.order_original_currency = 'EUR'
						THEN ROUND(A.order_original_amt / COALESCE(E.usd_to_eur, (SELECT usd_to_eur FROM reference.exchange_rate WHERE "date" = (SELECT MAX("date") FROM reference.exchange_rate))), 2)
					WHEN A.order_original_currency = 'MXN'
						THEN ROUND(A.order_original_amt / COALESCE(E.usd_to_mxn, (SELECT usd_to_mxn FROM reference.exchange_rate WHERE "date" = (SELECT MAX("date") FROM reference.exchange_rate))), 2)
					WHEN A.order_original_currency = 'GBP'
						THEN ROUND(A.order_original_amt / COALESCE(E.usd_to_gbp, (SELECT usd_to_gbp FROM reference.exchange_rate WHERE "date" = (SELECT MAX("date") FROM reference.exchange_rate))), 2)
				END AS order_converted_amt,
				A.order_converted_currency,
				A.order_original_amt,
				A.order_original_currency,
				A.order_payment_method,
				CAST(NULL AS VARCHAR) AS order_platform,
				COALESCE(A.product_name, S.product_name) AS product_name,
				COALESCE(P.product_type, S.product_type) AS product_type,
				COALESCE(CAST(D.nebula_id AS VARCHAR), A.user_id, S.user_id) AS user_id,
				A.subscription_id,
				CAST(NULL AS INT) AS product_renewal_number
			FROM order_with_parced_json AS A
			LEFT JOIN gold.fact_subscription_order AS S
				ON A.order_id = S.order_id AND A.order_status = S.order_status
			LEFT JOIN reference.exchange_rate AS E
				ON A.order_created_at = E."date"
			LEFT JOIN reference.country_code AS C
				ON LOWER(A.order_country) = LOWER(C.alpha_3_code)
			LEFT JOIN reference.product_mapping AS P
				ON A.product_name = P.product_name
			LEFT JOIN reference.user_id_mapping AS D
				ON A.user_id = D.id
			WHERE A.order_status IS NOT NULL
			)

			SELECT
				A.*
			FROM order_preprocessed AS A
			QUALIFY ROW_NUMBER() OVER(PARTITION BY order_sk ORDER BY order_created_at DESC) = 1
			);
			""",
        split_statements=True,
        conn_id='aws_redshift_cluster',
        autocommit=True
    )

    create_silver_webhook_app_order_incremental = SQLExecuteQueryOperator(
        task_id=f'create_silver_webhook_app_order_incremental',
        sql=f"""
			SET query_group TO 'slow-queue';
			DROP TABLE IF EXISTS silver.webhook_app_order_incremental;
			CREATE TABLE silver.webhook_app_order_incremental AS
			(
			WITH deduplicated_orders AS (
				SELECT
					CAST(transaction_id AS VARCHAR) AS order_id,
					CAST(transaction_type AS VARCHAR) AS order_status,
					CASE
						WHEN REGEXP_COUNT(purchase_date, '^[0-9]+$')
							THEN timestamp 'epoch' + CAST(purchase_date AS BIGINT) / 1000 * interval '1 second'
						ELSE CAST(purchase_date AS TIMESTAMP)
					END AS order_created_at,
					CAST('USD' AS VARCHAR) AS order_converted_currency,
					CASE
						WHEN trial = 1 OR CAST(renewal_number AS INT) = 0 THEN CAST(0 AS NUMERIC(18, 10))
						ELSE CAST(usd_gross AS NUMERIC(18, 10))
					END AS order_original_amt,
					CAST('USD' AS VARCHAR) AS order_original_currency,
					CAST('store_payment' AS VARCHAR) AS order_payment_method,
					CAST(COALESCE(platform,source) AS VARCHAR) AS order_platform,
					CAST(country AS VARCHAR) AS order_country,
					CAST(product_id AS VARCHAR) AS product_name,
					CAST(user_id AS VARCHAR) AS user_id,
					CAST(renewal_number AS INT) AS product_renewal_number,
					CASE 
						WHEN transaction_type IN ('income') THEN 1
						WHEN transaction_type IN ('outcome') THEN 2
					END AS order_status_priority
				FROM bronze.webhook_app_order AS A
				WHERE transaction_type IN ('income', 'outcome')
					AND COALESCE(platform, source) IN ('ios', 'android')
					AND product_id NOT IN ('chat_balance_recurrent', 'chat_balance_card', 'chat_balance_applepay', 'chat_balance_paypal', 'chat_balance_googlepay')
					AND (TIMESTAMP 'epoch' +  (event_created_at::bigint / 1000) * INTERVAL '1 second')::date >= DATEADD(day, -{variables["webhook_app_time_delta"]}, current_date)
				QUALIFY ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY event_time::bigint DESC, order_status_priority DESC) = 1
			),
			approved_orders AS (
				SELECT
					order_id,
					'approved' AS order_status,
					order_created_at,
					order_converted_currency,
					order_original_amt,
					order_original_currency,
					order_payment_method,
					order_platform,
					order_country,
					product_name::VARCHAR,
					user_id,
					product_renewal_number
				FROM deduplicated_orders AS A
			),
			refunded_orders AS (
				SELECT
					order_id,
					'refunded' AS order_status,
					DATEADD(second, 1, order_created_at) AS order_created_at,
					order_converted_currency,
					order_original_amt,
					order_original_currency,
					order_payment_method,
					order_platform,
					order_country,
					product_name::VARCHAR,
					user_id,
					product_renewal_number
				FROM deduplicated_orders AS A
				WHERE order_status IN ('outcome')
			),
			all_orders AS (
				SELECT * FROM approved_orders
				UNION ALL
				SELECT * FROM refunded_orders
			),
			enriched_orders AS (
				SELECT
					FARMFINGERPRINT64(COALESCE(A.order_id, 'NULL') || COALESCE(A.order_status, 'NULL')) AS order_sk,
					A.order_id,
					A.order_status,
					A.order_created_at,
					CASE
						WHEN A.order_original_currency = 'USD'
							THEN ROUND(A.order_original_amt, 2)
						WHEN A.order_original_currency = 'EUR'
							THEN ROUND(A.order_original_amt / COALESCE(E.usd_to_eur, (SELECT usd_to_eur FROM reference.exchange_rate WHERE "date" = (SELECT MAX("date") FROM reference.exchange_rate))), 2)
						WHEN A.order_original_currency = 'MXN'
							THEN ROUND(A.order_original_amt / COALESCE(E.usd_to_mxn, (SELECT usd_to_mxn FROM reference.exchange_rate WHERE "date" = (SELECT MAX("date") FROM reference.exchange_rate))), 2)
						WHEN A.order_original_currency = 'GBP'
							THEN ROUND(A.order_original_amt / COALESCE(E.usd_to_gbp, (SELECT usd_to_gbp FROM reference.exchange_rate WHERE "date" = (SELECT MAX("date") FROM reference.exchange_rate))), 2)
					END AS order_converted_amt,
					A.order_converted_currency,
					A.order_original_amt,
					A.order_original_currency,
					A.order_payment_method,
					A.product_name::VARCHAR,
					A.order_platform,
					A.order_country,
					CASE
						WHEN D.id IS NOT NULL THEN CAST(D.nebula_id AS VARCHAR)
						ELSE A.user_id
					END AS user_id,
					NULL AS subscription_id,
					A.product_renewal_number
				FROM all_orders AS A
				LEFT JOIN reference.user_id_mapping AS D
					ON A.user_id = D.id
				LEFT JOIN reference.exchange_rate AS E
					ON A.order_created_at = E."date"
			),
			final_card_order AS (
				SELECT
					A.order_sk,
					A.order_id,
					A.order_status,
					A.order_created_at,
					A.order_country,
					A.order_converted_amt,
					A.order_converted_currency,
					A.order_original_amt,
					A.order_original_currency,
					A.order_payment_method,
					A.order_platform,
					A.product_name::VARCHAR,
					CASE
						WHEN D.product_type IS NOT NULL THEN CAST(D.product_type AS VARCHAR)
						ELSE CAST(NULL AS VARCHAR)
					END AS product_type,
					A.user_id,
					A.subscription_id,
					A.product_renewal_number
				FROM enriched_orders AS A
				LEFT JOIN reference.product_mapping AS D
					ON A.product_name = D.product_name
			)
			SELECT * FROM final_card_order
			);
			""",
        split_statements=True,
        conn_id='aws_redshift_cluster',
        autocommit=True
    )

    insert_gold_fact_marketing_order = SQLExecuteQueryOperator(
        task_id=f'insert_gold_fact_marketing_order',
        sql=f"""
			SET query_group TO 'slow-queue';
			INSERT INTO gold.fact_marketing_order 
				SELECT 
					origin_table.* 
				FROM (
					SELECT * FROM silver.solid_webhook_card_order
					UNION ALL
					SELECT * FROM silver.solid_webhook_apm_order WHERE order_sk NOT IN (SELECT order_sk FROM silver.solid_webhook_card_order)
					UNION ALL
					SELECT * FROM silver.webhook_app_order_incremental
					UNION ALL
					SELECT 
						A.* 
					FROM silver.solid_webhook_subscription_order AS A
					WHERE 
						order_sk NOT IN (SELECT order_sk FROM silver.solid_webhook_card_order) AND
						order_sk NOT IN (SELECT order_sk FROM silver.solid_webhook_apm_order)
				) as origin_table
				WHERE origin_table.order_sk NOT IN (SELECT DISTINCT order_sk FROM gold.fact_marketing_order AS target_table);
			""",
        split_statements=True,
        conn_id='aws_redshift_cluster',
        autocommit=True
    )

    end_operator = EmptyOperator(task_id="dag_end", outlets=[Dataset(f"s3://prod-vmart-redshift-analytics/dataset/{dag_id}.txt")])

    start_operator >> \
    create_silver_solid_webhook_subscription_order >> insert_gold_solid_subscription_order >> \
    [create_silver_solid_webhook_card_order, create_silver_solid_webhook_apm_order, create_silver_webhook_app_order_incremental] >> \
    insert_gold_fact_marketing_order >> \
    end_operator