import json
import os
from datetime import date, datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

from utils.slack_callbacks import dag_failure_slack_alert

dag_id = "dag_bronze_solid_report"
current_date = date.today()
json_variables_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"variables/{dag_id}.json")

with open(json_variables_path, 'r') as j:
    variables = json.loads(j.read())

bucket_id = variables['bucket_id']


def get_secrets():
    from airflow.models import Variable

    return {
        "solid-merchants": Variable.get("solid-merchants", default_var="undefined", deserialize_json=True)
    }


with DAG(
        dag_id=dag_id,
        start_date=datetime(2025, 2, 1),
        schedule='10 * * * *',
        max_active_runs=1,
        description='DAG for incrementally loading Solidgate API reports into Redshift',
        tags=['prod', 'bronze', 'marketing', 'solid', 'orders'],
        default_args={
            'owner': 'airflow',
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
			"on_failure_callback": dag_failure_slack_alert
        },
        catchup=False
) as dag:
    start_operator = EmptyOperator(task_id="dag_start")

    with TaskGroup(group_id="solid_reports_to_redshift_load_group") as solid_reports_to_redshift_load_group:
        for operation, mapping in variables["mappings"].items():
            s3_filepath = f'{current_date}/{dag_id}/{operation}'


            '''
            This task is designed to remove previous .json files in S3
            '''
            remove_s3_keys = S3DeleteObjectsOperator(
                task_id=f"remove_s3_keys_for_{operation}",
                bucket=bucket_id,
                prefix=f"{s3_filepath}",
                aws_conn_id="aws_default"
            )


            '''
            This task is designed to extract reports from Solid API into S3 .json files using Lambda
            '''
            load_solid_api_to_s3 = LambdaInvokeFunctionOperator(
                task_id=f"load_solid_api_to_s3_for_{operation}",
                function_name=variables["lambda_function"],
                payload=json.dumps({
                    "s3_bucket": variables["bucket_id"],
                    "s3_filepath": s3_filepath,
                    "report_type": operation,
                    "merchants": mapping["merchants"],
                    "date_from": str(current_date - timedelta(days=mapping["time_delta"])) if mapping["time_delta"] != 'AUTO' else str(current_date - timedelta(days=3)),
                    "date_to": str(current_date + timedelta(days=1)),
                    "granularity_hours": mapping["granularity_hours"],
                    "fields": mapping["fields"]
                }),
                botocore_config={
                    "connect_timeout": 900,
                    "read_timeout": 900,
                    "tcp_keepalive": True
                }
            )

            '''
            This task is designed to truncate Staging layer table
            '''
            truncate_staging_table = SQLExecuteQueryOperator(
                task_id=f'truncate_staging_table_for_{operation}',
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
                task_id=f'load_s3_to_redshift_staging_for_{operation}',
                table=f'staging.{mapping["target_table_name"]}',
                copy_options=["json 'auto'"],
                s3_bucket=bucket_id,
                s3_key=s3_filepath,
                schema=f"vmart",
                redshift_conn_id="aws_redshift_cluster",
                aws_conn_id="aws_default"
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
                task_id=f'insert_data_into_bronze_layer_for_{operation}',
                sql=f'''
                    SET QUERY_GROUP TO 'slow-queue';					
                    INSERT INTO {mapping["target_table_dataset"]}.{mapping["target_table_name"]}
                    (
                        SELECT origin_table.*
                        FROM (
                            SELECT DISTINCT
                                FARMFINGERPRINT64(
                                    {' || '.join(['COALESCE(CAST("{0}" AS VARCHAR), {1})'.format(column, "'NULL'") for column in mapping["fields"]
                                                  if column not in mapping["non_fingerprint_fields"]])}
                                ) AS fingerprint,
                                {', '.join([f'"{column}"' for column in mapping["fields"]])}
                            FROM
                                staging.{mapping["target_table_name"]}
                            ORDER BY {mapping["source_incremental_column"]}
                            ) AS origin_table
                            WHERE origin_table.fingerprint NOT IN (SELECT DISTINCT fingerprint FROM {mapping["target_table_dataset"]}.{mapping["target_table_name"]} AS target_table)
                    );
                    ''',
                split_statements=True,
                conn_id='aws_redshift_cluster',
                autocommit=True
            )

            remove_s3_keys >> load_solid_api_to_s3 >> truncate_staging_table >> load_s3_to_redshift_staging >> insert_data_into_bronze_layer

    end_operator = EmptyOperator(task_id="dag_end", outlets=[Dataset(f"s3://prod-vmart-redshift-analytics/dataset/{dag_id}.txt")])

    start_operator >> solid_reports_to_redshift_load_group >> end_operator