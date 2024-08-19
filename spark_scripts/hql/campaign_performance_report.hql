alter table raw.googleads_campaign_performance add if not exists partition (data_date='{data_date}');

WITH api_data AS (
	select
		customer_currency_code as account_currency_code,
		customer_time_zone as account_time_zone,
		segments_ad_network_type as ad_network_type,
		campaign_advertising_channel_sub_type as advertising_channel_subtype,
		campaign_advertising_channel_type as advertising_channel_type,
		cast (campaign_budget_amount_micros as bigint) as amount,
		campaign_base_campaign AS base_campaignid,
		campaign_bidding_strategy AS bidding_strategy_id,
		bidding_strategy_name as bidding_strategy_name,
		campaign_bidding_strategy_type as bidding_strategy_type,
		campaign_campaign_budget as budget_id,
		cast (campaign_id AS bigint) as campaign_id,
		campaign_name,
		campaign_status,
		campaign_experiment_type as campaign_trial_type,
		customer_descriptive_name as account_descriptive_name,
		segments_day_of_week as day_of_week,
		segments_device as device,
		campaign_end_date as end_date,
		cast(customer_id AS bigint) as external_customer_id,
		campaign_final_url_suffix as final_url_suffix,
		campaign_budget_has_recommended_budget as has_recommended_budget,
		campaign_budget_explicitly_shared as is_budget_explicitly_shared,
		cast(campaign_maximize_conversion_value_target_roas AS double) AS maximize_conversion_value_target_roas,
		segments_month as month,
		campaign_budget_period as period,
		segments_quarter as quarter,
		cast(campaign_budget_recommended_budget_amount_micros AS bigint) AS recommended_budget_amount,
		campaign_serving_status as serving_status,
		campaign_start_date as start_date,
		cast (campaign_budget_total_amount_micros as bigint) as total_amount,
		campaign_tracking_url_template as tracking_url_template,
		campaign_url_custom_parameters as url_custom_parameters,
		segments_week as week,
		segments_year as year,
		cast(segments_date as date) as report_dt,
		cast(metrics_absolute_top_impression_percentage AS double) AS absolute_top_impression_percentage,
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
		cast(metrics_content_budget_lost_impression_share AS double) AS content_budget_lost_impression_share,
		cast(metrics_content_impression_share AS double) AS content_impression_share,
		cast(metrics_content_rank_lost_impression_share AS double) AS content_rank_lost_impression_share,
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
		cast(metrics_current_model_attributed_conversions_from_interactions_rate as double) as current_model_attributed_conversions_from_interactions_rate,
		cast(metrics_current_model_attributed_conversions_from_interactions_value_per_interaction as double) as current_model_attributed_conversions_from_interactions_value_per_interaction,
		cast(metrics_current_model_attributed_conversions_value AS double) AS current_model_attributed_conversion_value,
		cast(metrics_current_model_attributed_conversions_value_per_cost as double) as current_model_attributed_conversions_value_per_cost,
		cast(metrics_engagement_rate as double) as engagement_rate,
		cast(metrics_engagements AS bigint) AS engagements,
		cast(metrics_gmail_forwards AS int) AS gmail_forwards,
		cast(metrics_gmail_saves AS int) AS gmail_saves,
		cast(metrics_gmail_secondary_clicks as int) as gmail_secondary_clicks,
		cast(metrics_impressions AS bigint) as impressions,
		metrics_interaction_event_types as interaction_types,
		cast(metrics_interaction_rate as double) as interaction_rate,
		cast(metrics_interactions AS bigint) AS interactions,
		cast(metrics_search_absolute_top_impression_share AS double) AS search_absolute_top_impression_share,
		cast(metrics_search_budget_lost_absolute_top_impression_share AS double) AS search_budget_lost_absolute_top_impression_share,
		cast(metrics_search_budget_lost_impression_share as double) as search_budget_lost_impression_share,
		cast(metrics_search_budget_lost_top_impression_share AS double) AS search_budget_lost_top_impression_share,
		cast(metrics_search_click_share AS double) AS search_click_share,
		cast(metrics_search_exact_match_impression_share AS double) AS search_exact_match_impression_share,
		cast(metrics_search_impression_share AS double) AS  search_impression_share,
		cast(metrics_search_rank_lost_absolute_top_impression_share AS double) AS search_rank_lost_absolute_top_impression_share,
		cast(metrics_search_rank_lost_impression_share AS double) AS search_rank_lost_impression_share,
		cast(metrics_search_rank_lost_top_impression_share AS double) AS search_rank_lost_top_impression_share,
		cast(metrics_search_top_impression_share AS double) AS search_top_impression_share,
		cast(metrics_top_impression_percentage as double) as top_impression_percentage,
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
	FROM  
		raw.googleads_campaign_performance
	where 
		customer_currency_code != 'customer_currency_code' 
		and data_date='{data_date}'
),
--Note: Once customer id is deactivated we can't get data for that customer even when customer activated
deactivated_customer_data AS (
    SELECT 
        *
    FROM
        mart_mops.googleads_campaign_performance
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
INSERT OVERWRITE TABLE mart_mops.googleads_campaign_performance
PARTITION(data_date)
select
    *
from  
    source_data;
