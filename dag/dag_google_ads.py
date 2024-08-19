import base64
import os
import zlib
from datetime import timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.models import Variable
from mops_google_api_migration.dag_utils import prepare_deactivated_accounts_task, prepare_s3_file_delete_task
from mops_google_api_migration.dag_utils import prepare_hive_task
from mops_google_api_migration.dag_utils import prepare_download_task
from mops_google_api_migration.dag_utils import publish_availability_task, \
    ValidDataSensor, ValidateNewDataInstanceAndPublishEvaluationsOperator, publish_entity_availability_task
from airflow.operators.python_operator import PythonVirtualenvOperator


def pack_string(json_str):
    """
    Builds value to be passed as command line arg to a shell command.
    like spark-submit.
    :param json_str: a json
    :type json_str: str
    :rtype: str
    """
    j_str = "" if json_str is None else json_str
    return base64.b64encode(zlib.compress(j_str.encode('utf-8'), 9)). \
        decode('utf-8')


def get_task(configs, hql_artifactory_path, zacs_role, table_name, current_date, lookback_days):
    # DAG default arg
    parallel_task = configs.get("daily").get("parallel_task", configs.get("parallel_task"))
    daily = True

    spark_task_ads_data = prepare_hive_task(env,
                                            the_dag,
                                            hive_spark_script,
                                            zodiac_config, k,
                                            hql_artifactory_path,
                                            zacs_role,
                                            current_date,
                                            table_name,
                                            lookback_days,
                                            task_id=k + "_load_table")
    clear_the_raw_data = prepare_s3_file_delete_task(env,
                                                     the_dag,
                                                     packed_config,
                                                     s3_file_delete_script,
                                                     zodiac_config, k,
                                                     zacs_role,
                                                     current_date,
                                                     task_id=k + "_clear_the_raw_data")
    if parallel_task == 'True':
        parallel_task_num = configs.get("daily").get("parallel_task_num",1)
        for parallel_task_id in range(0,parallel_task_num):
            loading_ads_api_data = prepare_download_task(env,
                                                         the_dag,
                                                         daily,
                                                         packed_config,
                                                         ads_api_spark_script,
                                                         zodiac_config, k,
                                                         hql_artifactory_path,
                                                         zacs_role,
                                                         current_date,
                                                         str(parallel_task_id),
                                                         task_id=k + "_api_download_task_" + str(parallel_task_id))
            loading_ads_api_data.set_downstream(spark_task_ads_data)
            clear_the_raw_data.set_downstream(loading_ads_api_data)
    else:
        loading_ads_api_data = prepare_download_task(env,
                                                     the_dag,
                                                     daily,
                                                     packed_config,
                                                     ads_api_spark_script,
                                                     zodiac_config, k,
                                                     hql_artifactory_path,
                                                     zacs_role,
                                                     current_date,
                                                     '0',
                                                     task_id=k + "_api_download")
        loading_ads_api_data.set_downstream(spark_task_ads_data)
        clear_the_raw_data.set_downstream(loading_ads_api_data)

    hive_tables = configs["tables"]

    if "lcp" in configs.keys():
        publish_availability = publish_availability_task(env=env, dag=the_dag, conf=configs,
                                                         task_id=f"publish_availability_{str(hive_tables).split('.')[1]}",
                                                         hive_table=hive_tables)
        evaluate_lcp_contract = ValidateNewDataInstanceAndPublishEvaluationsOperator(
            env,
            the_dag,
            configs,
            hive_tables,
            zodiac_config
        )
        validate_lcp_contract = ValidDataSensor(env, the_dag, configs, hive_tables)
        spark_task_ads_data.set_downstream(evaluate_lcp_contract)
        evaluate_lcp_contract.set_downstream(validate_lcp_contract)
        validate_lcp_contract.set_downstream(publish_availability)
        publish_availability.set_downstream(deactivated_accounts)
    else:
        publish_availability = publish_entity_availability_task(env=env, dag=the_dag, conf=configs,
                                                                task_id=f"publish_availability_{str(hive_tables).split('.')[1]}",
                                                                hive_table=hive_tables)
        spark_task_ads_data.set_downstream(publish_availability)
        publish_availability.set_downstream(deactivated_accounts)

    return the_dag


# Load configurations
env = Variable.get('env', default_var='stage')
print('env = {}'.format(env))
config = yaml.safe_load(open(Path(__file__).absolute()
                             .parent.joinpath('config/configs.yaml')))
print(f"Config:{config}")
config_file_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    f"config/configs.yaml")
config_str = open(config_file_path, 'r').read()
packed_config = pack_string(config_str)
script_version = Path(__file__).absolute() \
    .parent.joinpath('VERSION').read_text()
print(f"script_version:  {script_version}")
artifactory_path = config['common']['dag_artifactory_path'] + script_version
print(f"artifactory_path:  {artifactory_path}")
ads_api_spark_script = artifactory_path + '/spark_runner.py'
print(f"ads_api_spark_script:  {ads_api_spark_script}")
hive_spark_script = artifactory_path + '/hive_task.py'
print(f"hive_spark_script:  {hive_spark_script}")
deactivated_accounts_script = artifactory_path + '/deactivated_accounts.py'
print(f"deactivated_accounts_script:  {deactivated_accounts_script}")
s3_file_delete_script = artifactory_path + '/s3_file_delete.py'
print(f"s3_file_delete_script:  {s3_file_delete_script}")
zodiac_config = yaml.safe_load(open(Path(__file__).absolute()
                                    .parent.joinpath('config/zodiac.yaml')))
common_config = config['common']
env_config = config[env]
feed_config = config['feeds']
report_config = {}
default_args = {
    'owner': common_config['owner'],
    'start_date': common_config['start_date'],
    'email': env_config['email_id'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}
# DAG Definition
the_dag = DAG(dag_id="google_api_data_ingestion",
              default_args=default_args,
              schedule_interval=common_config['schedule_interval'],
              max_active_runs=1,
              concurrency=20,
              catchup=False
              )

deactivated_accounts = prepare_deactivated_accounts_task(env,
                                                         the_dag,
                                                         packed_config,
                                                         deactivated_accounts_script,
                                                         zodiac_config,
                                                         "{{ds}}",
                                                         secret_env_var=common_config['secret_env_var'],
                                                         task_id="notify_deactivated_accounts")
for k, v in feed_config.items():
    if feed_config.get(k).get("enabled", False):
        report_config[k] = {**common_config, **env_config, **feed_config[k]}
        print(f"report_config[k]: {report_config[k]}")
        hql_artifactory_path = os.path.join(report_config[k]['dag_artifactory_path'],
                                            script_version, f"{k}.hql")
        print(f"hql_artifactory_path =  {hql_artifactory_path}")
        zacs_role = report_config[k]['zacs_role']
        print(f"zacs_role == {zacs_role}")
        start_dates = report_config[k]['start_date']
        print(f"start_dates == {start_dates}")
        current_date = report_config[k]['current_date']
        print(f"current_date = {current_date}")
        table_name = report_config[k]['tables']
        print(f"table_name = {table_name}")
        lookback_days = feed_config.get(k).get("daily").get("lookback_days", common_config["lookback_days"])
        dag_id = "google_api_data_ingestion"
        if zacs_role != '':
            globals()[dag_id] = get_task(report_config[k], hql_artifactory_path, zacs_role, table_name, current_date, lookback_days)
