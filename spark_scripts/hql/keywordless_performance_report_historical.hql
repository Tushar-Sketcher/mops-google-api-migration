alter table raw.googleads_keywordless_query_report add if not exists partition (data_date='{data_date}');

INSERT OVERWRITE TABLE mart_mef.googleads_keywordless_query_report
PARTITION(data_date)
Select
customer_currency_code as currency,
customer_descriptive_name as account,
customer_time_zone as time_zone,
cast(ad_group_id as  bigint) as ad_group_id,
ad_group_name as  ad_group,
ad_group_status as ad_group_state,
cast(metrics_all_conversions_from_interactions_rate as  double) as all_conv_rate,
cast(metrics_all_conversions as  double) as all_conv,
cast(metrics_all_conversions_value as  double) as all_conv_value,
cast(metrics_average_cpc as  double) as avg_cpc,
cast(metrics_average_cpm as  double) as avg_cpm,
cast(campaign_id as  bigint) as campaign_id,
campaign_name as campaign,
campaign_status as campaign_state,
cast(metrics_clicks as  bigint) as clicks,
cast(metrics_conversions_from_interactions_rate as  double) as conv_rate,
cast(metrics_conversions as  double) as conversions,
cast(metrics_cost_micros as  bigint) as cost,
cast(metrics_cost_per_all_conversions as  double) as cost_all_conv,
cast(metrics_cost_per_conversion as  double) as cost_conv,
segments_webpage as keyword_id ,
cast(metrics_cross_device_conversions as double) as cross_device_conv,
cast(metrics_ctr as double) as ctr,
customer_descriptive_name as client_name,
cast(segments_date as date) as  day ,
cast(customer_id as bigint) as customer_id,
cast(metrics_impressions as bigint) as impressions,
dynamic_search_ads_search_term_view_headline as dynamically_generated_headline ,
dynamic_search_ads_search_term_view_search_term as search_term ,
dynamic_search_ads_search_term_view_landing_page as url,
cast(metrics_value_per_all_conversions as double) as value_all_conv,
cast(metrics_value_per_conversion as double) as value_conv,
segments_date as data_date
from raw.googleads_keywordless_query_report
where data_date='{data_date}';