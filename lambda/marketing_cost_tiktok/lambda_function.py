import asyncio
import json
import logging
import concurrent.futures

import boto3

from tiktok_client import TikTokClient
from tiktok_data_loader import TikTokDataLoader

logging.getLogger().setLevel(logging.INFO)

def get_secret(secret_name: str, region_name="eu-west-1"):
    """Fetches a secret from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return json.loads(response["SecretString"])  # Returns secret as a dictionary
    except Exception as e:
        logging.error(f"Error retrieving secret: {e}")
        raise Exception(f"ERROR: {e}")

async def main(event):
    try:
        TIKTOK_ACCESS_TOKEN = get_secret("airflow/variables/tiktok-ads")['TT_ACCESS_TOKEN']

        tt_client = TikTokClient(TIKTOK_ACCESS_TOKEN, event["date_from"], event["date_to"])
        data_loader = TikTokDataLoader(event['s3_bucket'], event['s3_filepath'])

        all_transformed_data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(data_loader.fetch_and_process_report, tt_client,
                                advertiser["advertiserId"], advertiser["advertiserName"])
                for advertiser in event["advertiser_ids"]
            ]

            for future in concurrent.futures.as_completed(futures):
                transformed_data = future.result()
                if transformed_data:
                    all_transformed_data.extend(transformed_data)

            if all_transformed_data:
                await data_loader.process_and_upload_data(all_transformed_data)
    except Exception as e:
        logging.error(f"ERROR: {e}")
        raise Exception(f"ERROR: {e}")


def handler(event, context):
    return asyncio.run(main(event))
