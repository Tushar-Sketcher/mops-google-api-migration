alter table raw.googleads_ad_performance add if not exists partition (data_date='{data_date}');

WITH api_data AS (
    select
       customer_descriptive_name as account_descriptive_name,
       cast(ad_group_id AS bigint) AS ad_group_id,
       ad_group_name,
       ad_group_status,
       segments_ad_network_type as ad_network_type,
       cast(campaign_id AS bigint) as campaign_id,
       campaign_name,
       campaign_status,
       ad_group_ad_policy_summary_approval_status as creative_approval_status,
       ad_group_ad_ad_final_mobile_urls as creative_final_mobile_urls,
       ad_group_ad_ad_final_urls as creative_final_urls,
       ad_group_ad_ad_tracking_url_template as creative_tracking_url_template,
       ad_group_ad_ad_url_custom_parameters as creative_url_custom_parameters,
       cast(segments_date as date) as report_dt,
       ad_group_ad_ad_expanded_text_ad_description as description,
       ad_group_ad_ad_text_ad_description1 as description1,
       ad_group_ad_ad_text_ad_description2 as description2,
       segments_device as device,
       ad_group_ad_ad_device_preference as device_preference,
       ad_group_ad_ad_display_url as display_url,
       cast(customer_id AS bigint) as external_customer_id,
       ad_group_ad_ad_text_ad_headline as headline,
       ad_group_ad_ad_expanded_text_ad_headline_part1 as headline_part1,
       ad_group_ad_ad_expanded_text_ad_headline_part2 as headline_part2,
       cast(ad_group_ad_ad_id AS bigint) as id,
       ad_group_ad_status as status,
       ad_group_ad_ad_responsive_search_ad_headlines as responsive_search_ad_headlines,
       ad_group_ad_ad_responsive_search_ad_descriptions as responsive_search_ad_descriptions,
       ad_group_ad_ad_expanded_text_ad_headline_part3 as expanded_text_ad_headline_part3,
       ad_group_ad_ad_expanded_text_ad_description2 as expanded_text_ad_description2,
       cast(metrics_absolute_top_impression_percentage AS double) AS absolute_top_impression_prc,
       cast(metrics_active_view_cpm AS double) as active_view_cpm,
       cast(metrics_active_view_ctr AS double) as active_view_ctr,
      cast(metrics_active_view_impressions AS bigint) as active_view_impressions,
      cast(metrics_active_view_measurability AS double) as active_view_measurability,
      cast(metrics_active_view_measurable_cost_micros AS bigint) as active_view_measurable_cost,
      cast(metrics_active_view_measurable_impressions AS bigint) AS active_view_measurable_impressions,
      cast(metrics_active_view_viewability AS double) AS active_view_view_ability,
      cast(metrics_all_conversions AS double) AS all_conversions,
      cast(metrics_all_conversions_by_conversion_date AS double) AS all_conversions_by_conversion_date,
      cast(metrics_all_conversions_from_interactions_rate AS double) AS all_conversions_from_interactions_rate,
      cast(metrics_all_conversions_value AS double) AS all_conversion_value,
      cast(metrics_all_conversions_value_by_conversion_date AS double) AS all_conversions_value_by_conversion_date,
      cast(metrics_average_cost AS double) AS average_cost,
      cast(metrics_average_cpc AS double) AS average_cpc,
      cast(metrics_average_cpe AS double) AS average_cpe,
      cast(metrics_average_cpm AS decimal(30,6)) AS average_cpm,
      cast(metrics_average_cpv AS double) AS average_cpv,
      cast(metrics_clicks AS bigint) AS clicks,
      cast(metrics_conversions AS double) AS conversions,
      cast(metrics_conversions_by_conversion_date AS double) AS conversions_by_conversion_date,
      cast(metrics_conversions_from_interactions_rate AS double) AS conversions_from_interactions_rate,
      cast(metrics_conversions_value AS double) AS conversion_value,
      cast(metrics_conversions_value_by_conversion_date AS double) AS conversions_value_by_conversion_date,
      (cast(metrics_cost_micros as double)/1000000) as cost,
      cast(metrics_cost_per_all_conversions AS double) AS cost_per_all_conversions,
      cast(metrics_cost_per_conversion AS double) AS cost_per_conversion,
      cast(metrics_cost_per_current_model_attributed_conversion AS double) AS cost_per_current_model_attributed_conversion,
      cast(metrics_cross_device_conversions AS double) AS cross_device_conversions,
      cast(metrics_ctr as double) as ctr,
      cast(metrics_current_model_attributed_conversions AS double) AS current_model_attributed_conversions,
      cast(metrics_current_model_attributed_conversions_value AS double) AS current_model_attributed_conversion_value,
      cast(metrics_engagement_rate as double) as engagement_rate,
      cast(metrics_engagements AS bigint) AS engagements,
      cast(metrics_gmail_forwards AS int) AS gmail_forwards,
      cast(metrics_gmail_saves AS int) AS gmail_saves,
      cast(metrics_gmail_secondary_clicks as int) as gmail_secondary_clicks,
      cast(metrics_impressions AS bigint) as impressions,
      metrics_interaction_event_types as interaction_types,
      cast(metrics_interaction_rate as double) as interaction_rate,
      cast(metrics_interactions AS bigint) AS interactions,
       cast(metrics_top_impression_percentage as double) as top_impression_prc,
       cast(metrics_value_per_all_conversions as double) as value_per_all_conversions,
      cast(metrics_value_per_all_conversions_by_conversion_date as double) as value_per_all_conversions_by_conversion_date,
      cast(metrics_value_per_conversion as double) as value_per_conversion,
      cast(metrics_value_per_conversions_by_conversion_date as double) as value_per_conversions_by_conversion_date,
      cast(metrics_value_per_current_model_attributed_conversion as double) as value_per_current_model_attributed_conversion,
      cast(metrics_video_quartile_p100_rate as double) as video_quartile100_rate,
      cast(metrics_video_quartile_p25_rate as double) as video_quartile25_rate,
      cast(metrics_video_quartile_p50_rate as double) as video_quartile50_rate,
      cast(metrics_video_quartile_p75_rate as double) as video_quartile75_rate,
      cast(metrics_video_view_rate as double) as video_view_rate,
      cast(metrics_video_views AS int) AS video_views,
      cast(metrics_view_through_conversions AS int) AS view_through_conversions,
      segments_date as data_date
FROM raw.googleads_ad_performance
where customer_descriptive_name != 'customer_descriptive_name' and data_date='{data_date}'
),
--Note: Once customer id is deactivated we can't get data for that customer even when customer activated
deactivated_customer_data AS (
    SELECT 
        *
    FROM
        mart_mops.googleads_ad_performance
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
INSERT OVERWRITE TABLE mart_mops.googleads_ad_performance
PARTITION(data_date)
select
    *
from  
    source_data;
