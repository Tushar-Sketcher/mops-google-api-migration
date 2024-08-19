import base64
from airflow.operators.luminaire_contracts_plugin import PublishAvailabilityOperator
from airflow.operators.zacs_plugin import ZacsSparkSubmitOperator
from airflow.operators.luminaire_contracts_plugin import ValidDataInstanceSensor, ZacsValidateNewDataInstanceAndPublishEvaluationsOperator,PublishEntitiesAndAvailabilityOperator
from datetime import datetime, timedelta

def prepare_download_task(
        env, the_dag, daily, packed_config, ads_spark_script, zodiac_config,
        file_name, hql_artifactory_path, zacs_role, current_date, 
        parallel_task_id, task_id, execution_timeout=300, jars=None):
    """
    :param env: Prod/Stage
    :param the_dag: Airflow DAG parameters
    :param packed_config: Configuration from the configs.yaml file
    :param ads_spark_script: Name of the spark script to execute
    :param zodiac_config: configuration from zodiac.yaml file
    :param file_name: Feed name from the configuration (configs.yaml file)
    :param hql_artifactory_path: Artifactory path of the hql script to be executed
    :param zacs_role: AWS IAM role
    :param current_date: Date of processing
    :param task_id: Airflow task name
    :param jars: None
    :return: Return the Airflow DAG task
    """

    zacs_image = "analytics-docker.artifactory.zgtools.net/analytics/zacs/docker/spark/mops" \
                 "/mops-zacs-docker:spark-3.2.2_py-3.8.13_sc-2.12.15_jvm-11.0.16-google-api-migration-version-upgrade-new-125"

    spark_task = ZacsSparkSubmitOperator(
        task_id=task_id,
        zodiac_environment=env,
        zodiac_info=zodiac_config,
        image=zacs_image,
        storage_role_arn=zacs_role,
        spark_file=ads_spark_script,
        app_arguments=['--packed_config',
                       packed_config,
                       '--file_name',
                       file_name,
                       '--current_date',
                       current_date,
                       '--hql_artifactory_path',
                       hql_artifactory_path,
                       '--parallel_task_id',
                       parallel_task_id,
                       '--env',
                       env,
                       '--daily',
                       str(daily)
                       ],
        executor_memory='15g',
        driver_memory='30g',
        initial_executors='2',
        max_executors='12',
        execution_timeout=timedelta(minutes=execution_timeout),
        dag=the_dag,
        jars=jars,
        conf={'spark.driver.maxResultSize': '10g', 'spark.driver.memoryOverhead': '5g', "spark.reducer.maxReqsInFlight": 1,
              'spark.shuffle.io.retryWait': 60, 'spark.shuffle.io.maxRetries': 10,
              'spark.sql.shuffle.partitions': 1000, 'spark.dynamicAllocation.minExecutors': 1,
              'spark.dynamicAllocation.maxExecutors': 15},
        timeout=60
    )
    return spark_task

def prepare_s3_file_delete_task(
        env, the_dag, packed_config, s3_file_delete_script,
        zodiac_config, file_name, zacs_role, current_date, task_id, jars=None):
    """
    :param env: Prod/Stage
    :param the_dag: Airflow DAG parameters
    :param s3_file_delete_script: Name of the spark script to execute
    :param zodiac_config: configuration from zodiac.yaml file
    :param file_name: Feed name from the configuration (configs.yaml file)
    :param zacs_role: AWS IAM role
    :param current_date: Date of processing
    :param task_id: Airflow task name
    :param jars: None
    :return: Return the Airflow DAG task
    """

    zacs_image = "analytics-docker.artifactory.zgtools.net/analytics/zacs" \
                 "/docker/spark/mops/mops-zacs-docker:google-ads-49"

    spark_task = ZacsSparkSubmitOperator(
        task_id=task_id,
        zodiac_environment=env,
        zodiac_info=zodiac_config,
        image=zacs_image,
        storage_role_arn=zacs_role,
        spark_file=s3_file_delete_script,
        app_arguments=['--packed_config',
                       packed_config,
                       '--file_name',
                       file_name,
                       '--current_date',
                       current_date,
                       '--env',
                       env
                       ],
        executor_memory='5g',
        driver_memory='3g',
        initial_executors='1',
        max_executors='3',
        dag=the_dag,
        jars=jars,
        conf={"spark.reducer.maxReqsInFlight": 1, 'spark.shuffle.io.retryWait': 60, 'spark.shuffle.io.maxRetries': 10,
              'spark.sql.shuffle.partitions': 1000, 'spark.dynamicAllocation.minExecutors': 1,
              'spark.dynamicAllocation.maxExecutors': 3},
        timeout=60
    )
    return spark_task

def prepare_deactivated_accounts_task(env,the_dag,config,deactivated_accounts_script,zodiac_config,current_date,secret_env_var,task_id,execution_timeout=10):
    """
    :param env: Prod/Stage
    :param the_dag: Airflow DAG parameters
    :param config: Configuration from the configs.yaml file
    :param deactivated_accounts_script: Name of the spark script to execute
    :param zodiac_config: configuration from zodiac.yaml file
    :param current_date: Date of processing
    :param task_id: Airflow task name
    :param jars: None
    :return: Return the Airflow DAG task
    """

    zacs_image = "analytics-docker.artifactory.zgtools.net/analytics/zacs/docker/spark/mops" \
                 "/mops-zacs-docker:spark-3.2.2_py-3.8.13_sc-2.12.15_jvm-11.0.16-google-api-migration-version-upgrade-new-125"

    spark_task = ZacsSparkSubmitOperator(
        task_id=task_id,
        zodiac_environment=env,
        zodiac_info=zodiac_config,
        image=zacs_image,
        spark_file=deactivated_accounts_script,
        secret_env_var=secret_env_var,
        app_arguments=['--config',
                       config,
                       '--current_date',
                       current_date,
                       '--env',
                       env
                       ],
        executor_memory='3g',
        driver_memory='2g',
        initial_executors='1',
        max_executors='2',
        execution_timeout=timedelta(minutes=execution_timeout),
        dag=the_dag,
        timeout=60
    )
    return spark_task

def prepare_hive_task(
        env, the_dag, hive_spark_script,
        zodiac_config, file_name,
        hql_artifactory_path, zacs_role, current_date, table_name,
        lookback_days, task_id, execution_timeout=180, jars=None):
    """
    :param env: Prod/Stage
    :param the_dag: Airflow DAG parameters
    :param ads_spark_script: Name of the spark script to execute
    :param zodiac_config: configuration from zodiac.yaml file
    :param file_name: Feed name from the configuration (configs.yaml file)
    :param hql_artifactory_path: Artifactory path of the hql script to be executed
    :param zacs_role: AWS IAM role
    :param current_date: Date of processing
    :param task_id: Airflow task name
    :param lookback_days: lookback_days
    :param jars: None
    :return: Return the Airflow DAG task
    """

    zacs_image = "analytics-docker.artifactory.zgtools.net/analytics/zacs" \
                 "/docker/spark/mops/mops-zacs-docker:google-ads-49"

    spark_task = ZacsSparkSubmitOperator(
        task_id=task_id,
        zodiac_environment=env,
        zodiac_info=zodiac_config,
        image=zacs_image,
        storage_role_arn=zacs_role,
        spark_file=hive_spark_script,
        app_arguments=['--zacs_role',
                       zacs_role,
                       '--file_name',
                       file_name,
                       '--current_date',
                       current_date,
                       '--hql_artifactory_path',
                       hql_artifactory_path,
                       '--table_name',
                       table_name,
                       '--lookback_days',
                       lookback_days,
                       '--env',
                       env
                       ],
        executor_memory='10g',
        driver_memory='8g',
        initial_executors='3',
        max_executors='25',
        execution_timeout=timedelta(minutes=execution_timeout),
        dag=the_dag,
        jars=jars,
        conf={"spark.reducer.maxReqsInFlight": 1, 'spark.shuffle.io.retryWait': 60, 'spark.shuffle.io.maxRetries': 10,
              'spark.sql.shuffle.partitions': 1000, 'spark.dynamicAllocation.minExecutors': 3,
              'spark.dynamicAllocation.maxExecutors': 30},
        timeout=60
    )
    return spark_task

def publish_availability_task(env, dag, conf, task_id, hive_table):
    """
    :param env:Prod/Stage
    :param dag:Airflow DAG parameters
    :param conf:Configuration from the configs.yaml file
    :param task_id:Airflow task name
    :param hive_table: Table name for which we need to publish availability
    :return:Return the Airflow DAG task
    """
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
    api_token = conf["lcp_api"]
    # api_token = base64.b64decode(api_token)
    api_token = base64.b64decode(api_token + '=' * (-len(api_token) % 4))
    task_args = {
        'dag': dag,
        'task_id': task_id,
        'env': environment,
        'api_token': api_token,
        'entity_uid': entity_uid,
    }
    publish_availability = PublishAvailabilityOperator(**task_args)
    return publish_availability

def publish_entity_availability_task(env, dag, conf, task_id, hive_table):
    """
    :param env:Prod/Stage
    :param dag:Airflow DAG parameters
    :param conf:Configuration from the configs.yaml file
    :param task_id:Airflow task name
    :param hive_table: Table name for which we need to publish availability
    :return:Return the Airflow DAG task
    """
    environment = env
    if '.' in hive_table:
        table_name = hive_table.split('.')[1]
    else:
        table_name = hive_table
    entity_uid = 'zg-datalake-hive.hive.{}'.format(hive_table)
    contract_group_uid = table_name + ".daily"
    contract_uid = table_name + ".availability"
    instance_name = table_name + ".daily"
    team = 'mopsde'
    subscription_type = 'producer'
    curr_data_date = '{{ ds }}'
    next_data_date = '{{ next_ds }}'
    date_range = (curr_data_date,next_data_date)
    actions=[]
    print("data_date: " + str(curr_data_date) + ", next_data_date: " + str(next_data_date))
    api_token = conf["lcp_api"]
    # api_token = base64.b64decode(api_token)
    api_token = base64.b64decode(api_token + '=' * (-len(api_token) % 4))
    task_args = {
        'dag': dag,
        'task_id': task_id,
        'env': environment,
        'api_token': api_token,
        'entity_uid': entity_uid,
        'contract_group_uid': contract_group_uid,
        'contract_uid': contract_uid,
        'subscription_type': subscription_type,
        'instance_name': instance_name,
        'date_range': date_range,
        'actions': actions,
        'team': team,
    }
    publish_availability = PublishEntitiesAndAvailabilityOperator(**task_args)
    return publish_availability

def ValidDataSensor(env, sess_dag, brand_exp_config, hive_table):
    """
    :param env:Prod/Stage
    :param sess_dag:Airflow DAG parameters
    :param brand_exp_config:Configuration from the configs.yaml file
    :param hive_table: Table to which data has to be sensed and validated for lcp contracts defined
    :return:Return the Airflow DAG task
    """
    validate_lcp_contract = "validate_lcp_contract" + "_" + hive_table.split('.')[1]
    if brand_exp_config['current_date'] == '{{macros.ds_add(ds,-2)}}':
        return ValidDataInstanceSensor(
            entity_uid=brand_exp_config['lcp']['entity_uid'],
            instance_name=brand_exp_config['lcp']['instance_name'],
            at_time='{{macros.ds_add(ds,-2)}}',
            # at_time='{{ prev_ds }}',
            contract_group_uid=brand_exp_config['lcp']['contract_group_uid'],
            api_token=brand_exp_config['lcp']['api_token'],
            env=env,
            poke_interval=60,
            timeout=60,
            dag=sess_dag,
            task_id=validate_lcp_contract
        )
    else:
        return ValidDataInstanceSensor(
            entity_uid=brand_exp_config['lcp']['entity_uid'],
            instance_name=brand_exp_config['lcp']['instance_name'],
            at_time='{{ ds }}',
            # at_time='{{ prev_ds }}',
            contract_group_uid=brand_exp_config['lcp']['contract_group_uid'],
            api_token=brand_exp_config['lcp']['api_token'],
            env=env,
            poke_interval=60,
            timeout=60,
            dag=sess_dag,
            task_id=validate_lcp_contract
        )


def ValidateNewDataInstanceAndPublishEvaluationsOperator(env, sess_dag, brand_exp_config, hive_table, zodiac_config):
    """
    :param env:Prod/Stage
    :param sess_dag:Airflow DAG parameters
    :param brand_exp_config:Configuration from the configs.yaml file
    :param hive_table: Table name to which data has to be evaluated as per the lcp contracts defined
    :param zodiac_config:configuration from zodiac.yaml file
    :return:Return the Airflow DAG task
    """
    evaluate_lcp_contract = "evaluate_lcp_contract" + "_" + hive_table.split('.')[1]

    if brand_exp_config['current_date'] == '{{macros.ds_add(ds,-2)}}':
        partition_map = '{}:"{}"'.format(brand_exp_config['lcp']['partition_map'], brand_exp_config['current_date'])
        instance_start_date = '{{macros.ds_add(ds,-2)}}'
        instance_end_date = '{{ macros.ds_add(ds, -1) }}'

        return ZacsValidateNewDataInstanceAndPublishEvaluationsOperator(
            env=env,
            api_token=brand_exp_config['lcp']['api_token'],
            entity_uid=brand_exp_config['lcp']['entity_uid'],
            hive_table=brand_exp_config['tables'],
            contract_group_uid=brand_exp_config['lcp']['contract_group_uid'],
            partition_map=partition_map,
            pipeler_role_arn=brand_exp_config['aws_role_mef'],
            pipeler_jar=brand_exp_config['pipeler_jar'],
            instance_name=brand_exp_config['lcp']['instance_name'],
            instance_start_date=instance_start_date,
            instance_end_date=instance_end_date,
            driver_memory=brand_exp_config['lcp']['driver_memory'],
            zodiac_info=zodiac_config,
            dag=sess_dag,
            task_id=evaluate_lcp_contract
        )
    else:
        return ZacsValidateNewDataInstanceAndPublishEvaluationsOperator(
            env=env,
            api_token=brand_exp_config['lcp']['api_token'],
            entity_uid=brand_exp_config['lcp']['entity_uid'],
            hive_table=brand_exp_config['tables'],
            contract_group_uid=brand_exp_config['lcp']['contract_group_uid'],
            partition_map='{}:"{}"'.format(brand_exp_config['lcp']['partition_map'], '{{ ds }}'),
            # partition_map='snapshotdate:"{}"'.format('{{ ds }}'),
            pipeler_role_arn=brand_exp_config['aws_role_mef'],
            pipeler_jar=brand_exp_config['pipeler_jar'],
            instance_name=brand_exp_config['lcp']['instance_name'],
            # instance_start_date='{{ prev_ds }}',
            # instance_end_date='{{ ds }}',
            instance_start_date='{{ ds }}',
            instance_end_date='{{ next_ds }}',
            driver_memory=brand_exp_config['lcp']['driver_memory'],
            zodiac_info=zodiac_config,
            dag=sess_dag,
            task_id=evaluate_lcp_contract
        )

