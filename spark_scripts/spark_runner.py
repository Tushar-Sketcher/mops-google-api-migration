import argparse
import base64
import logging
import os
import zlib
import boto3
import pandas as pd
import yaml
from datetime import datetime, timedelta
import asyncio
import gc
import time
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


def unpack_string(value):
    """
    Unpacks original string from value returned by pack_string().
    :param value: a packed string value
    :type value: str
    :rtype: str
    """
    return zlib.decompress(base64.b64decode(value))

def _upload_file_to_s3(arn_role, bucket_name,
                       folder_name, current_date,
                       full_name, uploading_from):
    """
    :param arn_role: Role to access the S3 Bucket
    :param bucket_name: Bucket name where the file has to be placed
    :param file_name: File name by which the data from API has to be saved
    :param tgt_file_path: Target location
    :return: NA
    """
    sts_client = boto3.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn=arn_role,
        RoleSessionName="AWS-CLI-Session"
    )

    cred = assumed_role_object['Credentials']
    s3_resource = boto3.resource(
        's3',
        aws_access_key_id=cred['AccessKeyId'],
        aws_secret_access_key=cred['SecretAccessKey'],
        aws_session_token=cred['SessionToken'])
    print(f"UPLOADING FROM => {uploading_from}\n"
          f"UPLOADING TO => {bucket_name}/{folder_name}/"
          f"data_date={current_date}/{full_name}")
    try:
        s3_resource.Bucket(bucket_name).upload_file(f"{uploading_from}",
                                                    f"{folder_name}/"
                                                    f"data_date={current_date}/{full_name}")
    except Exception as e:
        print(f"Error Uploading file to S3 Bucket : {e}")
        raise e


async def get_all_reports(config, google_ads_client, client, file_name, hql_artifactory_path, env, task_id, parallel_task_id, report_settings, enum_data, active_customer_ids, deactivated_customer_ids, current_date, start_date, end_date, unique_dates=[]):
    """
    :param config: Configuration from the configs.yaml file
    :param google_ads_client: google ads client
    :param client: client
    :param file_name: Feed name from the configuration (configs.yaml file)
    :param hql_artifactory_path: Artifactory path of the hql script to be executed
    :param env: Prod/Stage
    :param task_id: async task number
    :param parallel_task_id: parallel task number
    :param active_customer_ids: active_customer_ids list
    :param deactivated_customer_ids: deactivated_customer_ids list
    :param current_date: Date of processing
    :param start_date: start date date of processing
    :param end_date: end date date of processing
    :return: NA
    """

    try:
        query = report_settings['query'].format(start_date=start_date, end_date=end_date)
        print(f"query == {query}")
        final_df = pd.DataFrame()
        print(f"Empty data frame initialized..{final_df}")
        data = dict()
        col_list = list()
        manager_customer_id_list = list()
        cols = query.replace("Select ", '')
        cols = cols.split('FROM')
        cols = cols[:-1]
        new_cols = cols[0].split(',')
        print(f"new_cols == {new_cols}")
        actual_cols = list()
        for col in new_cols:
            if col in enum_data.keys():
                actual_cols.append(enum_data[col])
            else:
                actual_cols.append("row." + col.strip())
        print(f"actual_cols == {actual_cols}")
        print("Parsing through each customer_id.....")
        active_customer_ids_duplicate=active_customer_ids.copy()
        for customer_id in active_customer_ids:
            print(f"customer_id == {customer_id}")
            flag = 0
            print(f"FLAG VALUE === {flag}")
            search_request = google_ads_client.get_type("SearchGoogleAdsStreamRequest")
            try:
                search_request.customer_id = customer_id
                search_request.query = query
                stream = client.search_stream(search_request)
                print(f"Running Query for customer_id == {customer_id}")
                current_report_name = file_name + f"_{customer_id}_parallel_task_{parallel_task_id}_async_task_{task_id}.parquet"
                print(f"current_report_name=={current_report_name}")

                print(os.listdir())

                file_dir = os.path.dirname(os.path.abspath(__file__))

                print(f"file_dir == {file_dir}")

                print(os.listdir(file_dir))

                file_path = os.path.join(file_dir, current_report_name)

                print(f"uploading_from == {file_path}")
                for batch in stream:
                    for row in batch.results:
                        for col_value in actual_cols:
                            query_value = eval(col_value)
                            if "google_ads_client" in col_value:
                                col = col_value.split('(')[1].replace(")", '').replace("row.", '').replace(".", '_')
                            else:
                                col = col_value.replace("row.", '').replace(".", '_')

                            if col in data.keys():
                                data[col].append(query_value)

                            else:
                                col_list.append(query_value)
                                data[col] = col_list
                                col_list = list()
                else:
                    if bool(data):
                        flag = 0
                        print("====== Creating DF =======")
                        final_df = pd.DataFrame(data)
                        df = final_df.astype(str)
                        unique_dates += df['segments_date'].unique().tolist()
                        df.to_parquet(file_path)
                        print("====== Converted DF to_parquet =======")
                    else:
                        flag = 1
                        print(f"== For this customer_id {customer_id} there is no data in stream ==")

                    try:
                        if flag == 0:
                            print("=======Starting Upload to S3=========")
                            _upload_file_to_s3(arn_role=config[str(env)]['aws_role'],
                                               bucket_name=config[str(env)]['raw_s3_bucket'],
                                               folder_name=report_settings['folder_path'],
                                               current_date=current_date,
                                               full_name=current_report_name,
                                               uploading_from=file_path)
                            print("===Report Successfully fetched & Uploaded to s3===")
                            del final_df
                            del df
                            gc.collect()
                        else:
                            print("====== There is no data to upload to S3, EMPTY FILE =======")
                    except Exception as e:
                        print(f"Error Occurred : {e}")
                        raise e
                    print("====== Clearing the DATA DICTIONARY =======")
                    data = dict()
                    print("====== Cleared the DATA DICTIONARY=======")
                    active_customer_ids_duplicate.remove(customer_id)
            except GoogleAdsException as ex:
                for error in ex.failure.errors:
                    if error.message == 'Metrics cannot be requested for a manager account. To retrieve metrics, issue separate requests against each client account under the manager account.':
                        manager_customer_id_list.append(customer_id)
                    else:
                        print(
                            f'Request with ID "{ex.request_id}" failed with status '
                            f'"{ex.error.code().name}" and includes the following errors:'
                            )
                        print(f'\tError with message "{error.message}".')
                        if error.location:
                            for field_path_element in error.location.field_path_elements:
                                print(f"\t\tOn field: {field_path_element.field_name}")
                        raise ex
    except Exception as e:
        print(f"Error : {e}".center(100, '='))
        print("re-pulling missing customer id's data")
        time.sleep(30)
        active_customer_ids = active_customer_ids_duplicate.copy()
        await get_all_reports(config, google_ads_client, client, file_name, hql_artifactory_path,
                              env, task_id, parallel_task_id, report_settings, enum_data, active_customer_ids,
                              deactivated_customer_ids, current_date, start_date, end_date, unique_dates)

    finally:
        if len(deactivated_customer_ids) > 0:
            print(f"Deactivated clients customer id's list: {deactivated_customer_ids}")
        print(f"Manager customer id's list: {manager_customer_id_list}")
    return unique_dates

async def main(config, file_name, parallel_task_id, hql_artifactory_path, current_date, env, daily):
    """
    :param env: Prod/Stage
    :param config: Configuration from the configs.yaml file
    :param file_name: Feed name from the configuration (configs.yaml file)
    :param hql_artifactory_path: Artifactory path of the hql script to be executed
    :param current_date: Date of processing
    :param parallel_task_id: parallel task number
    :return: NA
    """

    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s - %(levelname)s] %(message).5000s'
                        )
    logging.getLogger('google.ads.googleads.client').setLevel(logging.INFO)
    report_settings = config['feeds'][file_name]
    print(f"report_settings == {report_settings}")
    credentials = config['common']['credential']
    google_ads_client = GoogleAdsClient.load_from_dict(credentials, version="v16")
    client = google_ads_client.get_service("GoogleAdsService")
    active_customer_ids = []
    deactivated_customer_ids = []
    client_query = "SELECT customer_client.id, customer_client.status, customer_client.client_customer,  customer_client.resource_name  FROM customer_client"
    request = google_ads_client.get_type("SearchGoogleAdsRequest")
    request.customer_id = credentials["login_customer_id"]
    request.query = client_query
    enabled_response = client.search(request=request)

    for result in enabled_response.results:
        # 2 = ENABLED
        if str(result.customer_client.status) == '2':
            active_customer_ids.append(str(result.customer_client.id))
        else:
            deactivated_customer_ids.append(str(result.customer_client.id))
    print(f"active_customer_ids == {active_customer_ids}")
    print(f"deactivated_customer_ids == {deactivated_customer_ids}")
    print(f"credentials == {credentials}")

    enum_data = config['common']['enum_data']
    if daily == 'True':
        parallel_task = report_settings.get("daily").get("parallel_task", report_settings.get("parallel_task"))
        lookback_days = int(report_settings.get("daily").get("lookback_days", config["common"]["lookback_days"]))
        number_of_async_tasks = int(report_settings['daily']['number_of_async_tasks'])
        parallel_task_num = int(report_settings.get("daily").get('parallel_task_num', 1))
        parallel_async_lookback_days = int(report_settings.get("daily").get('parallel_async_lookback_days',1))
    else:
        parallel_task = report_settings.get("monthly").get("parallel_task", report_settings.get("parallel_task"))
        lookback_days = int(report_settings.get("monthly").get("lookback_days", config["common"]["monthly_lookback_days"]))
        number_of_async_tasks = int(report_settings['monthly']['number_of_async_tasks'])
        parallel_task_num = int(report_settings.get("monthly").get('parallel_task_num', 1))
        parallel_async_lookback_days = int(report_settings.get("monthly").get('parallel_async_lookback_days',1))

    base_date = current_date
    date_dict = {}
    unique_dates_list = list()
    parallel_base_date = ''

    # if parallel_task is true it perform parallel + async logic Otherwise it perform only async logic.
    if parallel_task == 'True':
        base_date = datetime.strptime(base_date, '%Y-%m-%d')

        # based on lookback_days, parallel_task_num and parallel_task_id it generates the parallel task start_date and end_date
        start_date = (base_date - timedelta(days=((int(parallel_task_id)+1) * (lookback_days / parallel_task_num))-1)).strftime("%Y-%m-%d")
        end_date = (base_date - timedelta(days=int(parallel_task_id) * (lookback_days / parallel_task_num))).strftime("%Y-%m-%d")
        print(f"parallel_start_date:{start_date} parallel_end_date:{end_date}")
        base_date = end_date
        lookback_days = parallel_async_lookback_days
    
    # based on parallel_async_lookback_days and number_of_async_tasks it generates async tasks start dates and end dates
    base_date = datetime.strptime(base_date, '%Y-%m-%d')
    for i in range(0,number_of_async_tasks):
        date_dict[i] = {}
        start_date = (base_date - timedelta(days=((i+1) * (lookback_days / number_of_async_tasks))-1)).strftime("%Y-%m-%d")
        end_date = (base_date-timedelta(days=i * (lookback_days / number_of_async_tasks))).strftime("%Y-%m-%d")
        print(f"start_date:{start_date} end_date:{end_date}")
        date_dict[i]['start_date'] = start_date
        date_dict[i]['end_date'] = end_date

    unique_dates_list += await asyncio.gather(*[get_all_reports(config, google_ads_client, client, file_name, hql_artifactory_path,
                                                                env, task_id, parallel_task_id, report_settings, enum_data, active_customer_ids,
                                                                deactivated_customer_ids, current_date, date_dict[task_id]['start_date'],
                                                                date_dict[task_id]['end_date']) for task_id in date_dict.keys()])

    dates_list=[]
    for row in unique_dates_list:
        dates_list.extend(row)
    print(f"dates_list: {dates_list}")
    unique_dates = pd.Series(dates_list).drop_duplicates().tolist()
    print(f"unique_dates: {unique_dates}")
    if not bool(report_settings.get("skip_validation", False)):
        if parallel_task == 'True':
            if daily == 'True':
                lookback_days = int(report_settings.get("daily").get('parallel_async_lookback_days'))
            else:
                lookback_days = int(report_settings.get("monthly").get('parallel_async_lookback_days'))
            current_date = parallel_base_date
        if len(unique_dates) != lookback_days:
            date_list = []
            date = datetime.strptime(current_date, '%Y-%m-%d')
            for i in range(lookback_days):
                date_list.append((date - timedelta(days=i)).strftime("%Y-%m-%d"))
            missing_list = set(date_list).difference(unique_dates)
            raise Exception(f"Missing data for this dates:{missing_list}")
    else:
        print('Validation is skipped.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Google_Ads_Api_Ingestion')
    parser.add_argument("--packed_config")
    parser.add_argument("--file_name")
    parser.add_argument("--current_date")
    parser.add_argument("--parallel_task_id")
    parser.add_argument("--hql_artifactory_path")
    parser.add_argument("--env")
    parser.add_argument("--daily")
    args = parser.parse_args()
    unpacked_config = unpack_string(args.packed_config)
    config = yaml.safe_load(unpacked_config)
    print(f"imported config... {config}")

    asyncio.run(main(config=config, file_name=args.file_name, parallel_task_id=args.parallel_task_id,
                     hql_artifactory_path=args.hql_artifactory_path, current_date=args.current_date,
                     env=args.env, daily=args.daily))
