alter table raw.googleads_search_query_performance add if not exists partition (data_date='{data_date}');

WITH api_data AS (
select
  customer_currency_code as currency_code,
  customer_descriptive_name as descriptive_name,
  customer_time_zone as time_zone,
  cast(ad_group_id AS bigint) as ad_group_id,
  ad_group_name ,
  ad_group_status ,
  segments_ad_network_type as ad_network_type,
  cast(metrics_all_conversions_from_interactions_rate as double) as all_conversions_from_interactions_rate ,
  cast(metrics_all_conversions as double) as all_conversions,
  cast(metrics_all_conversions_value as double) as all_conversions_value,
  cast(metrics_average_cost as double) as average_cost,
  cast(metrics_average_cpc as double) as  average_cpc,
  cast(metrics_average_cpm as double) as average_cpm,
  cast(metrics_top_impression_percentage as double) as  top_impression_percentage,
  cast(metrics_absolute_top_impression_percentage as double) as  absolute_top_impression_percentage,
  cast(campaign_id as bigint) as campaign_id ,
  campaign_name,
  campaign_status,
  cast(metrics_clicks as bigint) as clicks,
  cast(metrics_conversions as DOUBLE) as conversions ,
  cast(metrics_cost_micros as bigint) as  cost_micros,
  cast(ad_group_ad_ad_id as bigint) as ad_group_ad_ad_id,
  cast(metrics_cross_device_conversions as double) as  cross_device_conversions,
  cast(metrics_ctr as double) as ctr,
  cast(segments_date as date) as  report_dt,
  segments_day_of_week as day_of_week,
  segments_device as device,
  cast(customer_id as bigint) as external_customer_id,
  ad_group_ad_ad_final_urls,
  cast(metrics_impressions as bigint) as impressions,
  segments_month as month,
  segments_quarter quarter,
  search_term_view_search_term,
  segments_search_term_match_type as search_term_match_type,
  search_term_view_status,
  ad_group_ad_ad_tracking_url_template,
  segments_week as week,
  segments_year as year,
  cast(metrics_conversions_from_interactions_rate as double) as  conversions_from_interactions_rate,
  cast(metrics_conversions_value as double) as conversions_value,
  cast(metrics_cost_per_all_conversions as double) as cost_per_all_conversions,
  cast(metrics_cost_per_conversion as double) as cost_per_conversion,
  cast(metrics_interaction_rate as double) interaction_rate,
  metrics_interaction_event_types as interaction_event_types,
  cast(metrics_interactions as bigint) as interactions,
  cast(metrics_value_per_all_conversions as double) as value_per_all_conversions,
  cast(metrics_value_per_conversion as double) as value_per_conversion,
  cast(metrics_view_through_conversions as bigint) as view_through_conversions,
  segments_date as data_date
from 
  raw.googleads_search_query_performance
where 
  data_date='{data_date}'
),
--Note: Once customer id is deactivated we can't get data for that customer even when customer activated
deactivated_customer_data AS (
    SELECT 
        *
    FROM
        mart_mops.googleads_search_query_performance
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
INSERT OVERWRITE TABLE mart_mops.googleads_search_query_performance
PARTITION(data_date)
select
    *
from  
    source_data;
