alter table raw.googleads_topic_performance add if not exists partition (data_date='{data_date}');

INSERT OVERWRITE TABLE mart_mef.googleads_topic_performance
PARTITION(data_date)
select
ad_group_name,
cast(ad_group_id AS bigint) as ad_group_id,
ad_group_status,
segments_ad_network_type as ad_network_type,
cast(metrics_all_conversions AS double) AS all_conversions,
ad_group_criterion_approval_status as approval_status,
cast(ad_group_criterion_bid_modifier AS double) AS bid_modifier,
cast(campaign_id AS bigint) as campaign_id,
campaign_name,
campaign_status,
cast(metrics_clicks AS bigint) AS clicks,
cast(metrics_conversions AS double) AS conversions,
(cast(metrics_cost_micros as double)/1000000) as cost,
cast(ad_group_criterion_effective_cpc_bid_micros AS bigint) AS cpc_bid,
ad_group_criterion_effective_cpc_bid_source as cpc_bid_source,
ad_group_criterion_quality_info_creative_quality_score as creative_quality_score,
ad_group_criterion_topic_path as criteria,
ad_group_criterion_type_ as criteria_type,
cast(metrics_cross_device_conversions AS double) AS cross_device_conversions,
customer_descriptive_name,
segments_device as device,
cast(customer_id AS bigint) as external_customer_id,
ad_group_criterion_final_mobile_urls as final_mobile_urls,
ad_group_criterion_final_urls as final_urls,
cast(ad_group_criterion_position_estimates_first_page_cpc_micros AS bigint) AS first_page_cpc,
cast(ad_group_criterion_position_estimates_first_position_cpc_micros AS bigint) AS first_position_cpc,
cast(ad_group_criterion_criterion_id AS bigint) AS id,
cast(metrics_impressions AS bigint) as impressions,
ad_group_criterion_quality_info_post_click_quality_score as post_click_quality_score,
cast(ad_group_criterion_quality_info_quality_score AS bigint) as quality_score,
ad_group_criterion_quality_info_search_predicted_ctr as search_predicted_ctr,
ad_group_criterion_status as status,
cast(ad_group_criterion_position_estimates_top_of_page_cpc_micros AS bigint) as top_of_page_cpc,
ad_group_criterion_tracking_url_template as tracking_url_template,
ad_group_criterion_url_custom_parameters as url_custom_parameters,
cast(metrics_view_through_conversions AS int) AS view_through_conversions,
cast(segments_date as date) as report_dt,
segments_date as data_date
FROM  raw.googleads_topic_performance
where ad_group_name != 'ad_group_name' and data_date='{data_date}';

