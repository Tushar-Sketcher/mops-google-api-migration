import argparse

from pyspark.sql import SparkSession


def run_spark_job(arn_role, start_date, end_date, env):
    spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .appName("Loading Criteria Conv Report table") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

    spark._jsc.hadoopConfiguration().set("amz-assume-role-arn", arn_role)
    print(f"ENV === {env}")
    if env == 'prod':
        target_path = f"s3://serve-datalake-zillowgroup/zillowgroup/mops/google_ads/target/criteria_conv_report/"
    else:
        target_path = f"s3://dev-serve-datalake-zillowgroup/zillowgroup/mops/google_ads/target/criteria_conv_report/"
    print(f"start_date == {start_date} && end_date == {end_date}")
    try:
        df1 = spark.sql(f"select * from mart_mops.googleads_age_criteria_conv_report where data_date >= '{start_date}' and data_date <= '{end_date}'")

        df2 = spark.sql(f"select * from mart_mops.googleads_gender_criteria_conv_report where data_date >= '{start_date}' and data_date <= '{end_date}'")

        df3 = spark.sql(f"select * from mart_mops.googleads_user_criteria_conv_report where data_date >= '{start_date}' and data_date <= '{end_date}'")

        df4 = spark.sql(f"select * from mart_mops.googleads_topic_criteria_conv_report where data_date >= '{start_date}' and data_date <= '{end_date}'")

        df5 = spark.sql(f"select * from mart_mops.googleads_webpage_criteria_conv_report where data_date >= '{start_date}' and data_date <= '{end_date}'")

        df6 = spark.sql(f"select * from mart_mops.googleads_keyword_criteria_conv_report where data_date >= '{start_date}' and data_date <= '{end_date}'")

        df7 = spark.sql(f"select * from mart_mops.googleads_parent_criteria_conv_report where data_date >= '{start_date}' and data_date <= '{end_date}'")

        df8 = spark.sql(f"select * from mart_mops.googleads_income_criteria_conv_report where data_date >= '{start_date}' and data_date <= '{end_date}'")

        df9 = spark.sql(f"select * from mart_mops.googleads_placement_criteria_conv_report where data_date >= '{start_date}' and data_date <= '{end_date}'")

        df10 = df1.unionByName(df2, allowMissingColumns=True)
        df11 = df10.unionByName(df3, allowMissingColumns=True)
        df12 = df11.unionByName(df4, allowMissingColumns=True)
        df13 = df12.unionByName(df5, allowMissingColumns=True)
        df14 = df13.unionByName(df6, allowMissingColumns=True)
        df15 = df14.unionByName(df7, allowMissingColumns=True)
        df16 = df15.unionByName(df8, allowMissingColumns=True)
        df17 = df16.unionByName(df9, allowMissingColumns=True)

        df17.write.mode('overwrite').partitionBy("data_date").option("path", target_path).saveAsTable(
            "mart_mops.googleads_criteria_conv_report")

    except Exception as e:
        print("Error...{}".format(e))
        raise e

    finally:
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Criteria Conv Report Load')

    parser.add_argument("--arn_role", help="ARN Role to connect")
    parser.add_argument("--start_date")
    parser.add_argument("--end_date")
    parser.add_argument("--env")

    args = parser.parse_args()

    # trigger the Criteria Performance

    run_spark_job(arn_role=args.arn_role, start_date=args.start_date, end_date=args.end_date, env=args.env)
