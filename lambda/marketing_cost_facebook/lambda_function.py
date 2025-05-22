import json
import logging
from datetime import datetime

import boto3

from facebook_data_loader import FacebookDataLoader

logging.getLogger().setLevel(logging.ERROR)


def handler(event, context):
    facebook_secrets = json.loads(boto3.session.Session().client(service_name="secretsmanager").get_secret_value(
            SecretId="airflow/variables/facebook-ads")["SecretString"])

    facebook_data_loader = FacebookDataLoader(event["account_ids"],
                                            datetime.strptime(event["date_from"], "%Y-%m-%d"), datetime.strptime(event["date_to"], "%Y-%m-%d"),
                                            event["s3_bucket"], event["s3_filepath"], boto3.client("s3"),
                                            facebook_secrets["api_id"], facebook_secrets["api_secret"], facebook_secrets["api_access_token"],
                                            num_of_threads=256)
    return facebook_data_loader.load_facebook_data()
