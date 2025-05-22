import datetime
import logging
import time

from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from facebook_business.api import FacebookRequest


def catch(func, *args, **kwargs):
    max_retries = 6
    delay = 60

    for attempt in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except TimeoutError as e:
            logging.error(f"Attempt {attempt} - Timeout: {e}")
        except Exception as e:
            logging.error(f"Attempt {attempt} - Error: {e}")

        if attempt < max_retries:
            time.sleep(delay)


def execute_async_job(async_job: FacebookRequest):
    no_changes_count = 0
    last_completion_rate = 255

    async_job = async_job.execute()
    while True:
        async_job.api_get()

        if async_job[AdReportRun.Field.async_percent_completion] <= last_completion_rate:
            no_changes_count += 1
            if no_changes_count >= 255:
                print(f"{async_job[AdReportRun.Field.account_id]} {async_job[AdReportRun.Field.date_stop]}")
                raise TimeoutError('No completion changes for too long. \n{async_job}')

        if (async_job[AdReportRun.Field.async_status] == "Job Completed" and
                async_job[AdReportRun.Field.async_percent_completion] == 100):
            break

        elif async_job[AdReportRun.Field.async_status] == "Job Failed":
            print(f"{async_job[AdReportRun.Field.account_id]} {async_job[AdReportRun.Field.date_stop]}")
            raise Exception(f"Async job failed! \n{async_job}")

        no_changes_count = 0
        last_completion_rate = async_job[AdReportRun.Field.async_percent_completion]
        time.sleep(4)

    return [item.export_data() for item in async_job.get_result(params={"limit": 500})]


def get_ads_by_campaign(campaign_id: str, date_from: datetime, date_to: datetime):
    campaign = Campaign(fbid=campaign_id)

    i_async_job = campaign.get_insights(
        params={
            'level': AdsInsights.Level.ad,
            'time_range': {'since': date_from.strftime("%Y-%m-%d"), 'until': date_to.strftime("%Y-%m-%d")},
            'breakdowns': AdsInsights.Breakdowns.country,
            'time_increment': 1,
            'filtering': [{
                'field': 'ad.effective_status',
                'operator': 'IN',
                'value': [
                    Ad.EffectiveStatus.active,
                    Ad.EffectiveStatus.adset_paused,
                    Ad.EffectiveStatus.archived,
                    Ad.EffectiveStatus.campaign_paused,
                    Ad.EffectiveStatus.deleted,
                    Ad.EffectiveStatus.disapproved,
                    Ad.EffectiveStatus.in_process,
                    Ad.EffectiveStatus.paused,
                    Ad.EffectiveStatus.pending_billing_info,
                    Ad.EffectiveStatus.pending_review,
                    Ad.EffectiveStatus.preapproved,
                    Ad.EffectiveStatus.with_issues
                ]
            }]
        },
        fields=[
            AdsInsights.Field.account_name,
            AdsInsights.Field.account_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.adset_name,
            AdsInsights.Field.adset_id,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.ad_id,
            AdsInsights.Field.spend,
            AdsInsights.Field.clicks,
            AdsInsights.Field.impressions,
            AdsInsights.Field.account_currency,
            AdsInsights.Field.video_p25_watched_actions,
            AdsInsights.Field.video_p50_watched_actions,
            AdsInsights.Field.video_p75_watched_actions,
            AdsInsights.Field.video_p100_watched_actions
        ],
        is_async=True,
        pending=True)

    return catch(execute_async_job, i_async_job)

def get_campaigns(account_id: str, date_from: datetime, date_to: datetime) -> list[str]:
    account = AdAccount(f'act_{account_id}')
    async_job = account.get_insights(
        params={
            'level': AdsInsights.Level.campaign,
            'time_range': {'since': date_from.strftime("%Y-%m-%d"), 'until': date_to.strftime("%Y-%m-%d")},
            'time_increment': (date_to - date_from).days+1,
            # 'filtering': [{
            #     'field': 'campaign.effective_status',
            #     'operator': 'IN',
            #     'value': [
            #         Campaign.EffectiveStatus.active,
            #         Campaign.EffectiveStatus.archived,
            #         Campaign.EffectiveStatus.deleted,
            #         Campaign.EffectiveStatus.in_process,
            #         Campaign.EffectiveStatus.paused,
            #         Campaign.EffectiveStatus.with_issues
            #     ]
            # }]
        },
        fields=[
            AdsInsights.Field.campaign_id
        ],
        is_async=True,
        pending=True)

    campaigns = catch(execute_async_job, async_job)

    return [dict(item)['campaign_id'] for item in campaigns]