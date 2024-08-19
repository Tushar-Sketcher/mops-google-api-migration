alter table raw.googleads_paid_organic_query_report add if not exists partition (data_date='{data_date}');

WITH api_data AS (
    Select
        customer_currency_code as  currency_code,
        customer_descriptive_name as descriptive_name,
        customer_time_zone as time_zone,
        cast(ad_group_id AS bigint) AS group_id,
        ad_group_name AS ad_group_name,
        ad_group_status AS ad_group_status,
        cast(metrics_average_cpc AS double) AS average_cpc,
        cast(campaign_id as bigint) as campaign_id,
        campaign_name,
        campaign_status,
        cast(metrics_clicks AS bigint) AS clicks ,
        cast(metrics_combined_clicks AS bigint) AS combined_ads_organic_clicks,
        cast(metrics_combined_clicks_per_query AS double) AS combined_ads_organic_clicks_per_query,
        cast(metrics_combined_queries AS bigint) AS combined_ads_organic_queries,
        cast(metrics_ctr AS double) AS ctr ,
        cast(segments_date as date) as report_dt,
        segments_day_of_week AS day_of_week,
        cast(customer_id as bigint) AS external_customer_id,
        cast(metrics_impressions AS bigint) AS impressions ,
        segments_month AS month,
        cast(metrics_organic_clicks AS bigint) AS organic_clicks,
        cast(metrics_organic_clicks_per_query AS double) as organic_clicks_per_query,
        cast(metrics_organic_impressions AS bigint) AS organic_impressions,
        cast(metrics_organic_impressions_per_query AS double) AS organic_impressions_per_query,
        cast(metrics_organic_queries as bigint) AS organic_queries,
        segments_quarter AS quarter,
        paid_organic_search_term_view_search_term AS search_query,
        segments_search_engine_results_page_type AS serp_type,
        segments_week AS week,
        segments_year AS year,
        segments_date as data_date
        FROM raw.googleads_paid_organic_query_report
        where data_date='{data_date}'
),
--Note: Once customer id is deactivated we can't get data for that customer even when customer activated
deactivated_customer_data AS (
    SELECT 
        *
    FROM
        mart_mops.googleads_paid_organic_query_report
    WHERE
        data_date>='{start_date}'
        and data_date<='{end_date}'
        and external_customer_id not in (
            select 
                distinct(external_customer_id)
            from 
                api_data
        )
),
source_data as (
    select * from api_data
    UNION
    select * from deactivated_customer_data
)
INSERT OVERWRITE TABLE mart_mops.googleads_paid_organic_query_report
PARTITION(data_date)
select
    *
from  
    source_data;
