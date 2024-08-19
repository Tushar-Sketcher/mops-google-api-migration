from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.zacs_plugin import ZacsSparkSubmitOperator


def get_dag(config, start_dates, dag_ids):
    # DAG default args
    default_args = {
        'owner': "mopsde",
        'start_date': start_dates,
        'email': config[env]['email_id'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 4,
        'retry_delay': timedelta(minutes=3),
        'catchup': True
    }
    # DAG definition
    the_dag = DAG(
        dag_id=dag_ids,
        default_args=default_args,
        schedule_interval="0 7 * * *",
        max_active_runs=3
    )
    return the_dag


env = Variable.get('env', default_var='stage')
print(env)
# get config
config = yaml.safe_load(open(Path(__file__).absolute()
                             .parent.joinpath('config/configs.yaml')))
zodiac_config = yaml.safe_load(open(Path(__file__).absolute()
                                    .parent.joinpath('config/zodiac.yaml')))
config['zodiac_config'] = zodiac_config

if env == 'prod':
    config['arn_role'] = "arn:aws:iam::170606514770:" \
                         "role/mops-de-analytics-role"
    config['start_dates_conv'] = datetime(2020, 7, 1)
    config['dag_ids_conv'] = "criteria_conv_reports_july2020"
else:
    config['arn_role'] = "arn:aws:iam::170606514770:" \
                         "role/dev-mops-de-analytics-role"
    config['start_dates_conv'] = datetime(2021, 1, 1)
    config['dag_ids_conv'] = "criteria_conv_reports_historical"

config['dag_artifactory_path'] = \
    'https://artifactory.zgtools.net/artifactory/analytics-generic-local/' \
    'analytics/airflow/dags/big-data/mopsdag/mops-google-api-migration/'

print(config['arn_role'])
print(config['dag_artifactory_path'])
print(config['start_dates_conv'])
print(config['dag_ids_conv'])
# spark script and the related argument
script_version = Path(__file__).absolute().parent.joinpath('VERSION').read_text()
artifactory_path = config['dag_artifactory_path'] + script_version
criteria_conv_script = artifactory_path + '/criteria_conv_spark_task.py'
app_arguments = ['--arn_role', config['arn_role'], '--current_date', '{{ds}}', '--env', env]

# dag object
the_dag = get_dag(config, start_dates=config['start_dates_conv'], dag_ids=config['dag_ids_conv'])
zacs_image = "analytics-docker.artifactory.zgtools.net/analytics/zacs" \
             "/docker/spark/mops/mops-zacs-docker:google-ads-49"

# Dummy operator to starts off with
START_dummy_operator = DummyOperator(task_id="Start_Criteria_Conv_Load_july2020", dag=the_dag)

age_external_dependency = ExternalTaskSensor(
    external_dag_id="age_criteria_conv_reports_historical_july2020",
    external_task_id="age_criteria_conv_report_historical_hive_task",
    task_id='waiting_for_age_criteria_conv_report_historical_hive_task_to_complete',
    retries=30,
    poke_interval=20,
    timeout=600,
    dag=the_dag)

gender_external_dependency = ExternalTaskSensor(
    external_dag_id="gender_criteria_conv_reports_historical_july2020",
    external_task_id="gender_criteria_conv_report_historical_hive_task",
    task_id='waiting_for_gender_criteria_conv_report_historical_hive_task_to_complete',
    retries=30,
    poke_interval=20,
    timeout=600,
    dag=the_dag)

user_external_dependency = ExternalTaskSensor(
    external_dag_id="user_criteria_conv_reports_historical_july2020",
    external_task_id="user_criteria_conv_report_historical_hive_task",
    task_id='waiting_for_user_criteria_conv_report_historical_hive_task_to_complete',
    retries=30,
    poke_interval=20,
    timeout=600,
    dag=the_dag)

topic_external_dependency = ExternalTaskSensor(
    external_dag_id="topic_criteria_conv_reports_historical_july2020",
    external_task_id="topic_criteria_conv_report_historical_hive_task",
    task_id='waiting_for_topic_criteria_conv_report_historical_hive_task_to_complete',
    retries=30,
    poke_interval=20,
    timeout=600,
    dag=the_dag)

webpage_external_dependency = ExternalTaskSensor(
    external_dag_id="webpage_criteria_conv_reports_historical_july2020",
    external_task_id="webpage_criteria_conv_report_historical_hive_task",
    task_id='waiting_for_webpage_criteria_conv_report_historical_hive_task_to_complete',
    retries=30,
    poke_interval=20,
    timeout=600,
    dag=the_dag)

keyword_criteria_external_dependency = ExternalTaskSensor(
    external_dag_id="keyword_criteria_conv_reports_historical_july2020",
    external_task_id="keyword_criteria_conv_report_historical_hive_task",
    task_id='waiting_for_keyword_criteria_conv_report_historical_hive_task_to_complete',
    retries=30,
    poke_interval=20,
    timeout=600,
    dag=the_dag)

parent_external_dependency = ExternalTaskSensor(
    external_dag_id="parent_criteria_conv_reports_historical_july2020",
    external_task_id="parent_criteria_conv_report_historical_hive_task",
    task_id='waiting_for_parent_criteria_conv_report_historical_hive_task_to_complete',
    retries=30,
    poke_interval=20,
    timeout=600,
    dag=the_dag)

income_external_dependency = ExternalTaskSensor(
    external_dag_id="income_criteria_conv_reports_historical_july2020",
    external_task_id="income_criteria_conv_report_historical_hive_task",
    task_id='waiting_for_income_criteria_conv_report_historical_hive_task_to_complete',
    retries=30,
    poke_interval=20,
    timeout=600,
    dag=the_dag)

placement_external_dependency = ExternalTaskSensor(
    external_dag_id="placement_criteria_conv_reports_historical_july2020",
    external_task_id="placement_criteria_conv_report_historical_hive_task",
    task_id='waiting_for_placement_criteria_conv_report_historical_hive_task_to_complete',
    retries=30,
    poke_interval=20,
    timeout=600,
    dag=the_dag)

spark_task = ZacsSparkSubmitOperator(
    task_id="load_criteria_conv_report_historical",
    zodiac_environment=env,
    zodiac_info=config["zodiac_config"],
    image=zacs_image,
    storage_role_arn=config['arn_role'],
    spark_file=criteria_conv_script,
    app_arguments=app_arguments,
    executor_memory='10g',
    driver_memory='12g',
    initial_executors='2',
    max_executors='3',
    conf={"spark.reducer.maxReqsInFlight": 1, 'spark.shuffle.io.retryWait': 60, 'spark.shuffle.io.maxRetries': 10,
          'spark.sql.shuffle.partitions': 1000, 'spark.dynamicAllocation.minExecutors': 1,
          'spark.dynamicAllocation.maxExecutors': 3},
    dag=the_dag,
)

START_dummy_operator >> age_external_dependency >> gender_external_dependency >> user_external_dependency >> topic_external_dependency >> webpage_external_dependency >> keyword_criteria_external_dependency >> parent_external_dependency >> income_external_dependency >> placement_external_dependency >> spark_task
