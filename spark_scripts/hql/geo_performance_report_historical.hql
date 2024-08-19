alter table raw.googleads_geo_performance add if not exists partition (data_date='{data_date}');

INSERT OVERWRITE TABLE mart_mef.googleads_geo_performance
PARTITION(data_date)
select
customer_currency_code as account_currency_code,
customer_descriptive_name as account_descriptive_name,
customer_time_zone as account_time_zone,
cast(ad_group_id AS bigint) as ad_group_id,
ad_group_name,
ad_group_status,
segments_ad_network_type as ad_network_type,
cast(metrics_all_conversions_from_interactions_rate AS double) AS all_conversion_rate,
cast(metrics_all_conversions AS double) AS all_conversions,
cast(metrics_all_conversions_value AS double) AS all_conversion_value,
cast(metrics_average_cost AS double) AS average_cost,
cast(metrics_average_cpc AS double) AS average_cpc,
cast(metrics_average_cpm AS decimal(30,6)) AS average_cpm,
cast(metrics_average_cpv AS double) AS average_cpv,
cast (campaign_id AS bigint) as campaign_id,
campaign_name,
campaign_status,
cast(metrics_clicks AS bigint) AS clicks,
cast(metrics_conversions_from_interactions_rate AS double) AS conversions_rate,
cast(metrics_conversions AS double) AS conversions,
cast(metrics_conversions_value AS double) AS conversion_value,
(cast(metrics_cost_micros as double)/1000000) as cost,
cast(metrics_cost_per_all_conversions AS double) AS cost_per_all_conversions,
cast(metrics_cost_per_conversion AS double) AS cost_per_conversion,
cast(metrics_cross_device_conversions AS double) AS cross_device_conversions,
cast(metrics_ctr as double) as ctr,
segments_day_of_week as day_of_week,
segments_device as device,
cast(customer_id AS bigint) as external_customer_id,
cast(metrics_impressions AS bigint) as impressions,
cast(metrics_interaction_rate as double) as interaction_rate,
cast(metrics_interactions AS bigint) AS interactions,
metrics_interaction_event_types as interaction_types,
geographic_view_location_type as location_type,
segments_month as month,
segments_quarter as quarter,
cast(metrics_value_per_all_conversions as double) as value_per_all_conversions,
cast(metrics_value_per_conversion as double) as value_per_conversion,
cast(metrics_video_view_rate as double) as video_view_rate,
cast(metrics_video_views AS bigint) AS video_views,
cast(metrics_view_through_conversions AS bigint) AS view_through_conversions,
segments_week as week,
segments_year as year,
cast(segments_date as date) as report_dt,
segments_geo_target_city as city_criteria_id,
cty.cannonicalname as city_criteria_name,
geographic_view_country_criterion_id as country_criteria_id,
ctry.cannonicalname as country_criteria_name,
segments_geo_target_metro as metro_criteria_id,
met.cannonicalname as metro_criteria_name,
segments_geo_target_most_specific_location as most_specific_criteria_id,
mos.cannonicalname as most_specific_criteria_name,
segments_geo_target_region AS region_criteria_id,
rgn.cannonicalname as region_criteria_name,
segments_date as data_date
from raw.googleads_geo_performance a
join mart_mops.geo_constant_city cty ON (substr(a.segments_geo_target_city,20,7) = cty.id)
join mart_mops.geo_constant_country ctry ON (a.geographic_view_country_criterion_id = ctry.id)
join mart_mops.geo_constant_metro met ON (substr(a.segments_geo_target_metro,20,7) = met.id)
join mart_mops.geo_constant_most_specific mos ON (substr(a.segments_geo_target_most_specific_location,20,7) = mos.id)
join mart_mops.geo_constant_region rgn ON (substr(a.segments_geo_target_region,20,7) = rgn.id)
where customer_currency_code != 'customer_currency_code' and data_date='{data_date}';
