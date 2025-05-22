import asyncio
import json
import logging
from datetime import datetime

import aioboto3
from aiohttp import ClientSession

from solid_client import SolidClient


class ReportClient:
    def __init__(self,
                 solid_client: SolidClient,

                 report_type: str,
                 fields: list[str],
                 date_from: datetime,
                 date_to: datetime,
                 s3_bucket: str,
                 s3_filepath: str,
                 granularity_hours: int,
                 semaphore: asyncio.Semaphore):
        self.solid_client = solid_client

        self.report_type = report_type
        self.fields = fields
        self.date_from = date_from
        self.date_to = date_to
        self.s3_bucket = s3_bucket
        self.s3_filepath = s3_filepath
        self.granularity_hours = granularity_hours
        self.semaphore = semaphore

    @classmethod
    def set_up(cls,
               merchant_id: str,
               private_key: str,
               aiohttp_session: ClientSession,
               report_type: str,
               fields: list[str],
               date_from: datetime,
               date_to: datetime,
               s3_bucket: str,
               s3_filepath: str,
               granularity_hours: int,
               semaphore: asyncio.Semaphore):

        return ReportClient(
            SolidClient(merchant_id, private_key, aiohttp_session),
            report_type,
            fields,
            date_from,
            date_to,
            s3_bucket,
            s3_filepath,
            granularity_hours,
            semaphore
        )

    def _parse_order(self, order: dict) -> list[dict]:
        return [{field: order.get(field) for field in self.fields}]

    @staticmethod
    def _parse_subscription(subscription: dict) -> list[dict]:
        result = []

        for invoice in subscription.get("invoices", {}).values():
            for order in invoice.get("orders", {}).values():
                try:
                    row = {
                        "id": order["id"],
                        "status": order["status"],
                        "created_at": order["created_at"],
                        "processed_at": order["processed_at"],
                        "amount": order["amount"],
                        "operation": order.get("operation", None),
                        "retry_attempt": order["retry_attempt"],
                        "payment_details": order.get("payment_details", None),
                        "subscription": {k: v for k, v in subscription.items() if not isinstance(v, dict)},
                        "customer": subscription.get("customer", None),
                        "product": subscription.get("product", None),
                        "invoice": {k: v for k, v in invoice.items() if k != "orders"},
                        "pause": subscription.get("pause", None)
                    }
                    result.append(row)
                except Exception as e:
                    print(e)

        return result

    async def parse_reports(self):
        report_paths = {
            "apm": ("orders", "api/v1/apm-orders"),
            "card": ("orders", "api/v1/card-orders"),
            "subscription": ("subscriptions", "api/v1/subscriptions"),
        }

        operation, path = report_paths[self.report_type]

        result = []

        async with self.semaphore:
            async for report in self.solid_client.get_report(path, operation, self.date_from, self.date_to):
                if operation == "orders":
                    result.extend(self._parse_order(report))
                elif operation == "subscriptions":
                    result.extend(self._parse_subscription(report))

            if result:
                async with aioboto3.Session().client("s3") as s3:
                    await s3.put_object(
                        Bucket=self.s3_bucket,
                        Key=f"{self.s3_filepath}/{self.solid_client.merchant_id}_{self.date_from}.json",
                        Body="\n\n".join(json.dumps(item, indent=1) for item in result),
                        ContentType="application/json"
                    )

            logging.info(f"{self.solid_client.merchant_id}, {self.date_from}, {self.date_to}, {len(result)}")
