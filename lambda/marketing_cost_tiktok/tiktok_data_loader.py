import asyncio
import concurrent.futures
import json
import logging
from collections import defaultdict
from datetime import datetime

import aioboto3

from tiktok_client import TikTokClient

logging.getLogger().setLevel(logging.INFO)

class TikTokDataLoader:
    def __init__(self,  s3_bucket_id: str, s3_filepath: str, date_format: str = "%Y-%m-%d"):
        self.date_format = date_format
        self.s3_bucket_id = s3_bucket_id
        self.s3_filepath = s3_filepath

    def fetch_and_process_report(self, tt_client, advertiser_id: str, advertiser_name: str):
        """ Fetch data for an advertiser and process all pages concurrently."""
        page = 1
        transformed_data = []
        data, total_page = self.fetch_page_data(tt_client, advertiser_id, advertiser_name, page)
        transformed_data.extend(data)

        if total_page > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(self.fetch_page_data, tt_client, advertiser_id, advertiser_name, p)
                    for p in range(page+1, total_page + 1)
                ]
                for future in concurrent.futures.as_completed(futures):
                    data, total_pages = future.result()
                    transformed_data.extend(data)

            logging.info(f"Loaded all data for advertiser: {advertiser_id}")

        return transformed_data

    def fetch_page_data(self, tt_client: TikTokClient,
                           advertiser_id: str,
                           advertiser_name: str,
                           page: int) -> (list, int):
        metrics = [
            "campaign_name", "campaign_id", "adgroup_id", "adgroup_name", "ad_name",
            "spend", "impressions", "clicks", "mobile_app_id", "average_video_play",
            "video_watched_2s", "video_views_p25", "video_views_p50", "video_views_p75",
            "video_views_p100"
        ]
        dimensions = ["ad_id", "country_code", "stat_time_day"]
        date_level = "AUCTION_AD"
        report_type = "AUDIENCE"

        response = tt_client.create_ad_report(advertiser_id, report_type, dimensions, date_level, metrics, page)

        data = response['data']
        if 'list' in data and data['list'] is not None:
            logging.info(f"Found data for page {page}, len: {len(data['list'])}")
            return self.__transform_data(data['list'], advertiser_id, advertiser_name), int(data['page_info']['total_page'])
        else:
            return [], 1

    def __transform_data(self, data_list: list, advertiser_id, advertiser_name) -> list:
        TIKTOK_SPECIAL_ADS = ['7047596041469706241', '7221862608981262338', '7223718417654677506']

        result = []
        for item in data_list:
            dimensions = item["dimensions"]
            metrics = item["metrics"]
            formatted_date = datetime.strptime(dimensions["stat_time_day"], "%Y-%m-%d %H:%M:%S").strftime(self.date_format)
            transformed_item = {
                "ad_spend_time": formatted_date,
                "advertiser_id": advertiser_id,
                "advertiser_name": advertiser_name,
                "campaign_id": metrics["campaign_id"],
                "campaign_name": metrics["campaign_name"],
                "adset_id": metrics["adgroup_id"],
                "adset_name": metrics["adgroup_name"],
                "ad_id": dimensions["ad_id"],
                "ad_name": metrics["ad_name"],
                "spend": float(metrics["spend"]),
                "impressions": int(metrics["impressions"]),
                "clicks": int(metrics["clicks"]),
                "country_code": dimensions["country_code"],
                "video_watched_25": int(metrics["video_views_p25"]),
                "video_watched_50": int(metrics["video_views_p50"]),
                "video_watched_75": int(metrics["video_views_p75"]),
                "video_watched_100": int(metrics["video_views_p100"]),
                "video_watched_2s" : int(float(metrics["video_watched_2s"])),
                "average_video_play": int(float(metrics["average_video_play"]))
            }
            if advertiser_id in TIKTOK_SPECIAL_ADS:
                transformed_item["media_source"] = "TikTok_ads"
            else:
                transformed_item["media_source"] = "TikTok"

            result.append(transformed_item)

        return result

    async def process_and_upload_data(self, all_transformed_data: list):
        """ Splits data by event time and uploads asynchronously. """
        grouped_data = defaultdict(list)
        for item in all_transformed_data:
            event_time = item.get("ad_spend_time", "unknown_date")
            grouped_data[event_time].append(item)

        logging.info(f"Splitting data into {len(grouped_data)} files by event_time.")
        upload_tasks = [self.upload_to_s3(date_key, data_list) for date_key, data_list in grouped_data.items()]
        await asyncio.gather(*upload_tasks)

    async def upload_to_s3(self, report_date: str, result: list):
        try:
            # Extract year, month, day from report_date (assumes format YYYY-MM-DD)
            date_obj = datetime.strptime(report_date, self.date_format)
            year, month, day = date_obj.strftime("%Y"), date_obj.strftime("%m"), date_obj.strftime("%d")

            # Build partitioned S3 path
            s3_key = f"{self.s3_filepath}/{year}/{month}/{day}/tt_{report_date.replace('-', '_')}.json"
            async with aioboto3.Session().client("s3") as s3:
                await s3.put_object(
                    Bucket=self.s3_bucket_id,
                    Key=s3_key,
                    Body="\n\n".join(json.dumps(item, indent=1) for item in result),
                    ContentType="application/json"
                )
            logging.info(f"Finished uploading files to s3 for {report_date}")
        except Exception as e:
            logging.error(f"Failed to upload file to s3. Error: {e}")
            raise Exception(f"ERROR: {e}")
