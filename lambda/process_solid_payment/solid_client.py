import base64
import hashlib
import hmac
import json
from datetime import datetime
from typing import AsyncGenerator, Literal

import aiohttp


class SolidClient:
    def __init__(self,
                 merchant_id: str,
                 private_key: str,
                 session: aiohttp.ClientSession,
                 base_reconciliations_api_uri: str = "https://reports.solidgate.com/",
                 reconciliation_datetime_format: str = "%Y-%m-%d %H:%M:%S"):
        self.merchant_id = merchant_id
        self.private_key = private_key
        self.session = session
        self.base_reconciliations_api_uri = base_reconciliations_api_uri
        self.reconciliation_datetime_format = reconciliation_datetime_format

    async def get_report(self,
                         path: str,
                         operation: Literal["orders", "subscriptions", "disputes"],
                         date_from: datetime,
                         date_to: datetime) -> AsyncGenerator:
        full_url = self.base_reconciliations_api_uri + path
        attributes = {
            "date_from": date_from.strftime(self.reconciliation_datetime_format),
            "date_to": date_to.strftime(self.reconciliation_datetime_format)
        }

        while True:
            try:
                async with await self.__send_request(full_url, attributes) as response:

                    if 200 <= response.status < 300:
                        response_content = await response.json()
                        for order in response_content[operation]:
                            if operation == "orders":
                                yield order
                            else:
                                yield response_content[operation][order]

                        attributes["next_page_iterator"] = response_content["metadata"]["next_page_iterator"]
                        if not attributes["next_page_iterator"]:
                            break

                    else:
                        raise Exception(f"Request is not successful! Status Code: {response.status}")

            except aiohttp.ClientError as err:
                print(self.merchant_id, date_from, date_to)
                print(err)
                raise Exception(f"Exception during request: {err}")

    async def __send_request(self, path: str, attributes: dict) -> aiohttp.ClientResponse:
        body = json.dumps(attributes)
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Merchant": self.merchant_id,
            "Signature": self.__generate_signature(body)
        }
        return await self.session.post(path, headers=headers, json=attributes)

    def __generate_signature(self, data: str) -> str:
        encrypto_data = (self.merchant_id + data + self.merchant_id).encode("utf-8")
        sign = hmac.new(self.private_key.encode("utf-8"), encrypto_data, hashlib.sha512).hexdigest()
        return base64.b64encode(sign.encode("utf-8")).decode("utf-8")
