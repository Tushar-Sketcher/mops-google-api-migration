import argparse
import logging
from urllib.request import urlopen
from pyspark.sql import SparkSession
from datetime import datetime, timedelta


def _get_hql(hql_artifactory_path):
    try:
        response = urlopen(hql_artifactory_path)
        sql = response.read().decode('utf-8')
        return sql
    except Exception as e:
        logging.exception("Failed to get the SQL file from {}".format(hql_artifactory_path))
        raise e


def _update_hive_tables(zacs_role, file_name, current_date, hql_artifactory_path, table_name, lookback_days, env):
    app_name = "ADS API data Load{}".format(file_name)
    print(f" zacs_role == {zacs_role}")
    print(f" file_name == {file_name}")
    print(f" current_date == {current_date}")
    print(f" hql_artifactory_path == {hql_artifactory_path}")
    print(f" table name == {table_name}")
    print(f" lookback_days == {lookback_days}")
    print(f"env == {env}")
    hql_file = hql_artifactory_path.split("/")[-1]
    print(f" hql_file == {hql_file}")
    spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .appName(app_name) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("mapreduce.input.fileinputformat.input.dir.recursive", "true") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    # to allow parsing of inconsistent date string 'm/d/yyyy'
    # .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    # setting the configs
    logging.info(f"Acquiring role : {zacs_role}")
    spark._jsc.hadoopConfiguration().set("amz-assume-role-arn", zacs_role)
    logging.info(f"Role acquired : {zacs_role}")
    try:
        logging.info(f"HQL artifactory url is: {hql_artifactory_path}")
        sql = _get_hql(hql_artifactory_path)
        for sub_sql in sql.split(';'):
            logging.info(f"sql to execute: {sub_sql}")
            sub_sql = sub_sql.strip()
            # ignoring empty lines and property settings
            if sub_sql and not sub_sql.startswith("set "):
                # replacing placeholders after splitting to avoid unexpected splits.
                if sub_sql.startswith("WITH "):
                    base_date = datetime.strptime(current_date, '%Y-%m-%d')
                    start_date = (base_date - timedelta(days=int(lookback_days))).strftime("%Y-%m-%d")
                    sub_sql = sub_sql.format(start_date = start_date, end_date = current_date, data_date =current_date)
                else:
                    sub_sql = sub_sql.format(
                        data_date=current_date
                    )
                logging.info(f"final sql to execute: {sub_sql}")

                logging.info("Executing SQL: {sql}".format(sql=sub_sql))
                spark.sql(sub_sql)
                if len(table_name) == 0:
                    pass
                else:
                    spark.sql("MSCK REPAIR TABLE {table_name}".format(table_name=table_name))
    except Exception as e:
        logging.exception(f"Failed to update {hql_file} Hive Tables.")
        raise e
    finally:
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Google_Ads_Api_Ingestion')
    parser.add_argument("--zacs_role")
    parser.add_argument("--file_name")
    parser.add_argument("--current_date")
    parser.add_argument("--hql_artifactory_path")
    parser.add_argument("--table_name")
    parser.add_argument("--lookback_days")
    parser.add_argument("--env")
    args = parser.parse_args()
    _update_hive_tables(zacs_role=args.zacs_role,
                        file_name=args.file_name,
                        current_date=args.current_date,
                        hql_artifactory_path=args.hql_artifactory_path,
                        table_name=args.table_name,
                        lookback_days=args.lookback_days,
                        env=args.env
                        )
