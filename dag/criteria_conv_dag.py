from datetime import datetime, timedelta
from pathlib import Path
import base64
from airflow.operators.luminaire_contracts_plugin import PublishAvailabilityOperator
import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.zacs_plugin import ZacsSparkSubmitOperator, ZacsSparkSqlOperator
from airflow.operators.luminaire_contracts_plugin import DataAvailabilitySensor
from airflow.operators.luminaire_contracts_plugin import ValidDataInstanceSensor, \
    ZacsValidateNewDataInstanceAndPublishEvaluationsOperator


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
        'catchup': False
    }
    # DAG definition
    the_dag = DAG(
        dag_id=dag_ids,
        default_args=default_args,
        schedule_interval="0 14 * * *",
        max_active_runs=3,
        catchup=True
    )
    return the_dag


def ValidDataSensor(env, sess_dag, brand_exp_config, hive_table):
    validate_lcp_contract = "validate_lcp_contract" + "_" + hive_table.split('.')[1]
    return ValidDataInstanceSensor(
        entity_uid=brand_exp_config['feeds']["googleads_criteria_conv_report"]['lcp']['entity_uid'],
        instance_name=brand_exp_config['feeds']["googleads_criteria_conv_report"]['lcp']['instance_name'],
        at_time='{{ ds }}',
        # at_time='{{ prev_ds }}',
        contract_group_uid=brand_exp_config['feeds']["googleads_criteria_conv_report"]['lcp']['contract_group_uid'],
        api_token=brand_exp_config['feeds']["googleads_criteria_conv_report"]['lcp']['api_token'],
        env=env,
        poke_interval=60,
        timeout=60,
        dag=sess_dag,
        task_id=validate_lcp_contract
    )


def ValidateNewDataInstanceAndPublishEvaluationsOperator(env, sess_dag, brand_exp_config, hive_table, zodiac_config):
    evaluate_lcp_contract = "evaluate_lcp_contract" + "_" + hive_table.split('.')[1]
    return ZacsValidateNewDataInstanceAndPublishEvaluationsOperator(
        env=env,
        api_token=brand_exp_config['feeds']["googleads_criteria_conv_report"]['lcp']['api_token'],
        entity_uid=brand_exp_config['feeds']["googleads_criteria_conv_report"]['lcp']['entity_uid'],
        hive_table=brand_exp_config['feeds']["googleads_criteria_conv_report"]['tables'],
        contract_group_uid=brand_exp_config['feeds']["googleads_criteria_conv_report"]['lcp']['contract_group_uid'],
        partition_map='{}:"{}"'.format(brand_exp_config['feeds']["googleads_criteria_conv_report"]['lcp']['partition_map'],
                                       '{{ ds }}'),
        # partition_map='snapshotdate:"{}"'.format('{{ ds }}'),
        pipeler_role_arn=brand_exp_config[str(env)]['aws_role_mef'],
        pipeler_jar=brand_exp_config["common"]['pipeler_jar'],
        instance_name=brand_exp_config['feeds']["googleads_criteria_conv_report"]['lcp']['instance_name'],
        # instance_start_date='{{ prev_ds }}',
        # instance_end_date='{{ ds }}',
        instance_start_date='{{ ds }}',
        instance_end_date='{{ next_ds }}',
        driver_memory=brand_exp_config['feeds']["googleads_criteria_conv_report"]['lcp']['driver_memory'],
        zodiac_info=zodiac_config,
        dag=sess_dag,
        task_id=evaluate_lcp_contract
    )


def publish_availability_task(env, dag, conf, task_id, hive_table):
    environment = env
    if '.' in hive_table:
        table_name = hive_table.split('.')[1]
    else:
        table_name = hive_table
    entity_uid = 'zg-datalake-hive.hive.{}'.format(hive_table)
    contract_group_uid = table_name + ".daily"
    contract_uid = table_name + ".availability"
    instance_name = table_name + ".daily"
    curr_data_date = '{{ ds }}'
    next_data_date = '{{ next_ds }}'
    print("data_date: " + str(curr_data_date) + ", next_data_date: " + str(next_data_date))
    api_token = conf['feeds']["googleads_criteria_conv_report"]['lcp']["lcp_api"]
    # api_token = base64.b64decode(api_token)
    api_token = base64.b64decode(api_token + '=' * (-len(api_token) % 4))
    task_args = {
        'dag': dag,
        'task_id': task_id,
        'env': environment,
        'api_token': api_token,
        'entity_uid': entity_uid
    }
    publish_availability = PublishAvailabilityOperator(**task_args)
    return publish_availability


def msck_repair_table(env, hive_table, sess_dag, brand_exp_config, zodiac_config):
    return ZacsSparkSqlOperator(
        zodiac_environment=env,
        sql="MSCK REPAIR TABLE {table_name}".format(table_name=hive_table),
        zodiac_info=zodiac_config,
        storage_role_arn=brand_exp_config[str(env)]['aws_role_mef'],
        dag=sess_dag,
        task_id="msck_repair_table"
    )


env = Variable.get('env', default_var='stage')
print(env)
# get config
config = yaml.safe_load(open(Path(__file__).absolute()
                             .parent.joinpath('config/configs.yaml')))
zodiac_config = yaml.safe_load(open(Path(__file__).absolute()
                                    .parent.joinpath('config/zodiac.yaml')))
config['zodiac_config'] = zodiac_config

# each run loads the previous day data
config['current_date'] = "{{ds}}"
config['dag_ids'] = "criteria_conv_reports"
if env == 'prod':
    config['arn_role'] = "arn:aws:iam::170606514770:" \
                         "role/mops-de-analytics-role"
    config['start_dates'] = datetime(2022, 9, 1)
# config['dag_ids'] = "criteria_conv_reports"
else:
    config['arn_role'] = "arn:aws:iam::170606514770:" \
                         "role/dev-mops-de-analytics-role"
    config['start_dates'] = datetime(2021, 1, 1)
# config['dag_ids'] = "criteria_conv_reports_historical"

config['dag_artifactory_path'] = \
    'https://artifactory.zgtools.net/artifactory/analytics-generic-local/' \
    'analytics/airflow/dags/big-data/mopsdag/mops-google-api-migration/'
print(config['arn_role'])
print(config['dag_artifactory_path'])
print(config['start_dates'])
print(config['dag_ids'])
# spark script and the related argument
script_version = Path(__file__).absolute().parent.joinpath('VERSION').read_text()
artifactory_path = config['dag_artifactory_path'] + script_version
criteria_conv_script = artifactory_path + '/criteria_conv_spark_task.py'
app_arguments = ['--arn_role', config['arn_role'], '--end_date', '{{ds}}', '--start_date', '{{macros.ds_add(ds,-100)}}', '--env', env]

# dag object
the_dag = get_dag(config, start_dates=config['start_dates'], dag_ids=config['dag_ids'])
zacs_image = "analytics-docker.artifactory.zgtools.net/analytics/zacs" \
             "/docker/spark/mops/mops-zacs-docker:google-ads-49"

# Dummy operator to starts off with
START_dummy_operator = DummyOperator(task_id="Start_Criteria_Conv_Load", dag=the_dag)
range_start, range_end = '{{ ds }}', '{{ next_ds }}'


age_external_dependency = DataAvailabilitySensor(
    schema=config["feeds"]["age_criteria_conv_report"]["tables"].split('.')[0],
    table_name=config["feeds"]["age_criteria_conv_report"]["tables"].split('.')[1],
    date_range=(range_start, range_end),
    env=env,
    dag=the_dag,
    task_id='waiting_for_age_criteria_conv_report_load_table_to_complete')


gender_external_dependency = DataAvailabilitySensor(
    schema=config["feeds"]["gender_criteria_conv_report"]["tables"].split('.')[0],
    table_name=config["feeds"]["gender_criteria_conv_report"]["tables"].split('.')[1],
    date_range=(range_start, range_end),
    env=env,
    dag=the_dag,
    task_id='waiting_for_gender_criteria_conv_report_load_table_to_complete')

user_external_dependency = DataAvailabilitySensor(
    schema=config["feeds"]["user_criteria_conv_report"]["tables"].split('.')[0],
    table_name=config["feeds"]["user_criteria_conv_report"]["tables"].split('.')[1],
    date_range=(range_start, range_end),
    env=env,
    dag=the_dag,
    task_id='waiting_for_user_criteria_conv_report_load_table_to_complete')

topic_external_dependency = DataAvailabilitySensor(
    schema=config["feeds"]["topic_criteria_conv_report"]["tables"].split('.')[0],
    table_name=config["feeds"]["topic_criteria_conv_report"]["tables"].split('.')[1],
    date_range=(range_start, range_end),
    env=env,
    dag=the_dag,
    task_id='waiting_for_topic_criteria_conv_report_load_table_to_complete')

webpage_external_dependency = DataAvailabilitySensor(
    schema=config["feeds"]["webpage_criteria_conv_report"]["tables"].split('.')[0],
    table_name=config["feeds"]["webpage_criteria_conv_report"]["tables"].split('.')[1],
    date_range=(range_start, range_end),
    env=env,
    dag=the_dag,
    task_id='waiting_for_webpage_criteria_conv_report_load_table_to_complete')

keyword_criteria_external_dependency = DataAvailabilitySensor(
    schema=config["feeds"]["keyword_criteria_conv_report"]["tables"].split('.')[0],
    table_name=config["feeds"]["keyword_criteria_conv_report"]["tables"].split('.')[1],
    date_range=(range_start, range_end),
    env=env,
    dag=the_dag,
    task_id='waiting_for_keyword_criteria_conv_report_load_table_to_complete')

parent_external_dependency = DataAvailabilitySensor(
    schema=config["feeds"]["parent_criteria_conv_report"]["tables"].split('.')[0],
    table_name=config["feeds"]["parent_criteria_conv_report"]["tables"].split('.')[1],
    date_range=(range_start, range_end),
    env=env,
    dag=the_dag,
    task_id='waiting_for_parent_criteria_conv_report_load_table_to_complete')

income_external_dependency = DataAvailabilitySensor(
    schema=config["feeds"]["income_criteria_conv_report"]["tables"].split('.')[0],
    table_name=config["feeds"]["income_criteria_conv_report"]["tables"].split('.')[1],
    date_range=(range_start, range_end),
    env=env,
    dag=the_dag,
    task_id='waiting_for_income_criteria_conv_report_load_table_to_complete')

placement_external_dependency = DataAvailabilitySensor(
    schema=config["feeds"]["placement_criteria_conv_report"]["tables"].split('.')[0],
    table_name=config["feeds"]["placement_criteria_conv_report"]["tables"].split('.')[1],
    date_range=(range_start, range_end),
    env=env,
    dag=the_dag,
    task_id='waiting_for_placement_criteria_conv_report_load_table_to_complete')

spark_task = ZacsSparkSubmitOperator(
    task_id="load_criteria_conv_report",
    zodiac_environment=env,
    zodiac_info=config["zodiac_config"],
    image=zacs_image,
    storage_role_arn=config['arn_role'],
    spark_file=criteria_conv_script,
    app_arguments=app_arguments,
    executor_memory='10g',
    driver_memory='12g',
    initial_executors='5',
    max_executors='20',
    conf={"spark.reducer.maxReqsInFlight": 1, 'spark.shuffle.io.retryWait': 60, 'spark.shuffle.io.maxRetries': 10,
          'spark.sql.shuffle.partitions': 1000, 'spark.dynamicAllocation.minExecutors': 5,
          'spark.dynamicAllocation.maxExecutors': 20},
    dag=the_dag,
)

hive_tables = config["common"]["tables_criteria_conv"]
entity_uid = config['feeds']["googleads_criteria_conv_report"]['lcp']['entity_uid']
msck_repair_table = msck_repair_table(env, hive_tables, the_dag, config, zodiac_config)
publish_availability = publish_availability_task(env=env, dag=the_dag, conf=config,
                                                 task_id=f"publish_availability_{str(hive_tables).split('.')[1]}",
                                                 hive_table=hive_tables)
if hive_tables != '' and entity_uid != '':
    evaluate_lcp_contract = ValidateNewDataInstanceAndPublishEvaluationsOperator(env, the_dag, config,
                                                                                 hive_tables,
                                                                                 zodiac_config)
    validate_lcp_contract = ValidDataSensor(env, the_dag, config, hive_tables)
    START_dummy_operator >> [age_external_dependency, gender_external_dependency, user_external_dependency, topic_external_dependency, webpage_external_dependency, keyword_criteria_external_dependency, parent_external_dependency, income_external_dependency, placement_external_dependency] >> spark_task >> msck_repair_table >> evaluate_lcp_contract >> validate_lcp_contract >> publish_availability
else:
    START_dummy_operator >> [age_external_dependency, gender_external_dependency, user_external_dependency, topic_external_dependency, webpage_external_dependency, keyword_criteria_external_dependency, parent_external_dependency, income_external_dependency, placement_external_dependency]>> spark_task >> publish_availability
