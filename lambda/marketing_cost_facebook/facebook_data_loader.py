import json
from datetime import datetime, timedelta
import re
from typing import Generator

from facebook_business import FacebookAdsApi

from facebook_client import get_ads_by_campaign, get_campaigns
from thread_pool import ThreadPool

FB_API_VERSION = "v22.0"


class FacebookDataLoader:
    def __init__(self, accounts_ids: dict[str, list[str]], date_from: datetime, date_to: datetime, s3_bucket: str, s3_filepath: str, s3_client,
                 api_id: str, api_secret: str, api_access_token: str, api_version: str = FB_API_VERSION,
                 num_of_threads: int = 64):
        FacebookAdsApi.init(api_id, api_secret, api_access_token, api_version=api_version)

        self.accounts_ids = accounts_ids
        self.date_from = date_from
        self.date_to = date_to
        self.s3_bucket = s3_bucket
        self.s3_filepath = s3_filepath
        self.s3_client = s3_client
        self.thread_pool = ThreadPool(num_of_threads)

    @staticmethod
    def __generate_days(date_from: datetime, date_to: datetime, granularity_days: int = 1) -> Generator:
        current_day = date_from
        delta = timedelta(days=granularity_days)

        while current_day <= date_to:
            next_day = min(current_day + delta - timedelta(days=1), date_to)
            yield current_day, next_day
            current_day += delta

    def __upload_ads_to_s3(self, ads: list[dict[str, str]]):
        date = datetime.strptime(ads[0]["date_stop"], "%Y-%m-%d")
        account_id = ads[0]["account_id"]
        campaign_id = ads[0]["campaign_id"]
        
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=f"{self.s3_filepath}/{date.strftime('%Y/%m/%d')}/{account_id}_{campaign_id}_{date.strftime('%Y_%m_%d')}.json",
            Body="\n\n".join(json.dumps(ad, indent=1) for ad in ads),
            ContentType="application/json"
        )

    def __process_ad_media_source(self, ad):
        media_sources = {
            "social_facebook": "Social_facebook",
            "chat_capi": "Chat_CAPI",
            "brand": "Brand_campaign",
            "ad_quantum": "Ad_quantum"
        }

        for key, source in media_sources.items():
            if ad["account_id"] in self.accounts_ids[key]:
                return source

        if re.search(r"landing", ad["account_name"], re.IGNORECASE):
            return "Social_facebook"
            
        else:
            return "Facebook_ads"

    def __process_ads(self, response):
        return [{
            "date_stop": item["date_stop"],
            "account_id": item["account_id"],
            "account_name": item["account_name"],
            "campaign_id": item["campaign_id"],
            "campaign_name": item["campaign_name"],
            "adset_id": item["adset_id"],
            "adset_name": item["adset_name"],
            "ad_id": item["ad_id"],
            "ad_name": item["ad_name"],
            "spend": item["spend"],
            "account_currency": item["account_currency"],
            "impressions": item["impressions"],
            "clicks": item["clicks"],
            "country": item["country"],
            "video_p25_watched_actions": item["video_p25_watched_actions"][0]["value"] if item.get(
                "video_p25_watched_actions") else None,
            "video_p50_watched_actions": item["video_p50_watched_actions"][0]["value"] if item.get(
                "video_p50_watched_actions") else None,
            "video_p75_watched_actions": item["video_p75_watched_actions"][0]["value"] if item.get(
                "video_p75_watched_actions") else None,
            "video_p100_watched_actions": item["video_p100_watched_actions"][0]["value"] if item.get(
                "video_p100_watched_actions") else None,
            "media_source": self.__process_ad_media_source(item)
        }
            for item in response]

    def upload_insights_by_campaign(self, campaign_id: str, date_from: datetime, date_to: datetime):
        ads = get_ads_by_campaign(campaign_id, date_from, date_to)
        if ads:
            self.thread_pool.submit(self.__upload_ads_to_s3, self.__process_ads(ads))

    def upload_campaign_ads(self, account_id: str, date_from: datetime, date_to: datetime):
        campaigns = get_campaigns(account_id, date_from, date_to)
        for campaign in campaigns:
            self.thread_pool.submit(self.upload_insights_by_campaign, campaign, date_from, date_to)

    def load_facebook_data(self):
        for date_from, date_to in self.__generate_days(self.date_from, self.date_to):
            for account_id_list in self.accounts_ids.values():
                for account_id in account_id_list:
                    self.thread_pool.submit(self.upload_campaign_ads, account_id, date_from, date_to)
        self.thread_pool.shutdown()
