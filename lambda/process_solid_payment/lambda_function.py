import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Generator

import aiohttp
import boto3

from report_client import ReportClient

logging.getLogger().setLevel(logging.INFO)


def generate_hours(date_from: datetime, date_to: datetime, granularity_hours: int) -> Generator:
    current = date_from
    delta = timedelta(hours=granularity_hours)

    while current < date_to:
        next_interval = min(current + delta, date_to)
        yield current, next_interval
        current = next_interval


async def main(event):
    semaphore = asyncio.Semaphore(64)

    async def process_report(merchant_info, date_from, date_to):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=64, force_close=True)) as session:
            await ReportClient.set_up(merchant_info["merchant_id"], merchant_info["private_key"], session,
                                      event["report_type"], event["fields"],
                                      date_from, date_to,
                                      event["s3_bucket"], event["s3_filepath"],
                                      event["granularity_hours"],
                                      semaphore).parse_reports()

    all_merchants = json.loads(
        boto3.session.Session().client(service_name="secretsmanager").get_secret_value(SecretId="airflow/variables/solid-merchants")["SecretString"]
    )["merchants"]
    await asyncio.gather(*[asyncio.create_task(process_report(merchant_info, date_from, date_to))
                           for merchant, merchant_info in all_merchants.items() if merchant in event["merchants"]
                           for date_from, date_to in generate_hours(datetime.strptime(event["date_from"], "%Y-%m-%d"),
                                                 datetime.strptime(event["date_to"], "%Y-%m-%d"),
                                                 event["granularity_hours"])])

def handler(event, context):
    return asyncio.run(main(event))
