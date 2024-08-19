import argparse
import base64
import zlib
import boto3
import yaml


def unpack_string(value):
    """
    Unpacks original string from value returned by pack_string().
    :param value: a packed string value
    :type value: str
    :rtype: str
    """
    return zlib.decompress(base64.b64decode(value))

def delete_files_from_s3(config, file_name, current_date, env):
    """
    :param env: Prod/Stage
    :param config: Configuration from the configs.yaml file
    :param file_name: Feed name from the configuration (configs.yaml file)
    :param current_date: Date of processing
    :return: NA
    """
    arn_role = config[str(env)]['aws_role']
    print(f"arn_role == {arn_role}")
    bucket_name = config[str(env)]['raw_s3_bucket']
    folder_name = config['feeds'][file_name]['folder_path']

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

    bucket = s3_resource.Bucket(bucket_name)
    # delete all the resources inside a folder
    try:
        for obj in bucket.objects.filter(Prefix=f"{folder_name}/data_date={current_date}"):
            print('deleting...' + obj.key)
            obj.delete()
    except Exception as e:
        print(f"Error deleting files from S3 Bucket : {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Google_Ads_Api_Ingestion')
    parser.add_argument("--packed_config")
    parser.add_argument("--file_name")
    parser.add_argument("--current_date")
    parser.add_argument("--env")
    args = parser.parse_args()
    unpacked_config = unpack_string(args.packed_config)
    config = yaml.safe_load(unpacked_config)
    print(f"imported config... {config}")

    delete_files_from_s3(config=config, file_name=args.file_name, current_date=args.current_date,
                     env=args.env)
