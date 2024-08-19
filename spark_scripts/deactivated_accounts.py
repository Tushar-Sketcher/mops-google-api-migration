import argparse
import base64
import logging
import os
import zlib
import pandas as pd
import yaml
import requests
from google.ads.googleads.client import GoogleAdsClient

def send_slack_notification(config,current_date, env, token, channel_id):
    """
    :param env: Prod/Stage
    :param config: Configuration from the configs.yaml file
    :param current_date: Date of processing
    :param token: Airflow task name
    :param token: slack api key
    :param channel_id: slack channel id.
    """
    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s - %(levelname)s] %(message).5000s'
                        )
    logging.getLogger('google.ads.googleads.client').setLevel(logging.INFO)

    #Importing the config
    print(f"current_date  {current_date}")
    print(f"imported config... {config}")
    credentials = config['common']['credential']
    #Connecting to GoogleAdsClient API
    google_ads_client = GoogleAdsClient.load_from_dict(credentials, version="v16")
    client = google_ads_client.get_service("GoogleAdsService")
    consolidated_deactivated_customer_ids = {}
    #Running the query to get the list of active and deactivated accounts
    client_query = "SELECT customer_client.id, customer_client.status, customer_client.client_customer,  customer_client.resource_name  FROM customer_client"
    request = google_ads_client.get_type("SearchGoogleAdsRequest")
    request.customer_id = credentials["login_customer_id"]
    request.query = client_query
    enabled_response = client.search(request=request)
    for k in config['feeds'].keys():
        if config['feeds'].get(k).get("enabled", False):
            active_customer_ids = []
            deactivated_customer_ids = []
            for result in enabled_response.results:
                # 2 = ENABLED
                if str(result.customer_client.status) == '2':
                    active_customer_ids.append(str(result.customer_client.id))
                else:
                    deactivated_customer_ids.append(str(result.customer_client.id))
            if len(deactivated_customer_ids) > 0:
                consolidated_deactivated_customer_ids[k] = deactivated_customer_ids
            print(f"active_customer_ids == {active_customer_ids}")
            print(f"deactivated_customer_ids == {deactivated_customer_ids}")
    #Checking to see whether we have any deactivated accounts and send an alert if there are any
    if len(consolidated_deactivated_customer_ids) > 0:

        mail_body = ""
        for k, v in consolidated_deactivated_customer_ids.items():
            mail_body = mail_body + """  •  `{k}:{v}`\n""".format(k=k,v=v)
        message = """*Marketing DE | Deactivated GoogleAds Accounts {current_date}*\n_Hi Team - Following are the list of accounts that are deactivated/cancelled for the customer feeds as follows_\n{mail_body}""".format(
            current_date=current_date,mail_body=mail_body)
        print(f"Deactivated client's customer ids list {consolidated_deactivated_customer_ids}") 
        response = requests.post(
            'https://zillowgroup.slack.com/api/chat.postMessage',
            headers={
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            },
            json={
                'channel': channel_id,
                'text': message
            }
        )

        if response.status_code == 200:
            print('Message sent to Slack')
        else:
            print('Failed to send message to Slack:', response.text)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Google_Ads_Api_Deactivated_accounts')
    parser.add_argument("--config")
    parser.add_argument("--current_date")
    parser.add_argument("--env")
    args = parser.parse_args()
    unpacked_config = zlib.decompress(base64.b64decode(args.config))
    config = yaml.safe_load(unpacked_config)
    print(f"imported config... {config}")
    token = str(os.environ.get('token'))
    channel_id = str(os.environ.get('channel_id'))

    send_slack_notification(config=config, current_date=args.current_date, 
                    env=args.env, token=token, channel_id=channel_id)
