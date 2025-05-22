import json
import time
import logging
from datetime import datetime
import requests
from requests import RequestException

logging.getLogger().setLevel(logging.INFO)

BASE_TIKTOK_URL = 'https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/'


class TikTokClient:
    def __init__(self, access_token, date_from: str, date_to: str, date_format: str = "%Y-%m-%d"):
        self.access_token = access_token
        self.date_format = date_format
        self.date_from = self.__ensure_date_format(date_from)
        self.date_to = self.__ensure_date_format(date_to)

    def __ensure_date_format(self, date_str: str) -> str:
        return datetime.strptime(date_str, self.date_format).strftime(self.date_format)

    def create_ad_report(self,
                         advertiser_id: str,
                         report_type: str,
                         dimensions: list,
                         data_level: str,
                         metrics: list,
                         page="1",
                         page_size="1000") -> dict:
        filtering = [{
            "field_name": "ad_status",
            "filter_type": "IN",
            "filter_value": "[\"STATUS_ALL\"]"
        }]
        params = {
            "metrics": json.dumps(metrics),
            "dimensions": json.dumps(dimensions),
            "data_level": data_level,
            "report_type": report_type,
            "page_size": page_size,
            "page": str(page),
            "filtering": json.dumps(filtering)
        }
        headers = {
            'Access-Token': self.access_token,
        }
        payload = {
            'start_date': self.date_from,
            'end_date': self.date_to,
        }

        url = BASE_TIKTOK_URL + '?' + '&'.join(f"{k}={v}" for k, v in params.items())
        max_retries = 3
        retry_delay = 2
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, params={'advertiser_id': advertiser_id, **payload})

                if response.status_code == 200:
                    return json.loads(response.text)

                print(
                    f"Attempt {attempt + 1}: Received status code {response.status_code}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

            except RequestException as e:
                logging.info(f"Attempt {attempt + 1}: Request failed due to {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

        print("Max retries reached. Returning an empty dictionary.")
        return {}
