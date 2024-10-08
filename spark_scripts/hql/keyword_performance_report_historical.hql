alter table raw.googleads_keyword_performance add if not exists partition (data_date='{data_date}');

INSERT OVERWRITE TABLE mart_mef.googleads_keyword_performance
PARTITION(data_date)
select
    ad_group_criterion_keyword_text as criteria,
    cast(ad_group_criterion_criterion_id AS bigint) as id,
    ad_group_criterion_negative as is_negative,
    ad_group_criterion_keyword_match_type as keyword_match_type,
    ad_group_criterion_status as status,
    ad_group_criterion_final_urls as final_urls,
    customer_descriptive_name as account_descriptive_name,
    cast(customer_id AS bigint) as external_customer_id,
    cast(ad_group_id AS bigint) as ad_group_id,
    ad_group_name,
    ad_group_status,
    cast(campaign_id AS bigint) as campaign_id,
    campaign_name,
	campaign_status,
	cast(segments_date as date) as report_dt,
    cast(metrics_all_conversions AS double) AS all_conversions,
    ad_group_criterion_approval_status as approval_status,
    cast(metrics_top_impression_percentage as double) as top_impression_prc,
    cast(metrics_absolute_top_impression_percentage AS double) AS absolute_top_impression_prc,
    cast(metrics_average_page_views AS double) AS average_page_views,
	cast(metrics_average_cost AS double) AS average_cost,
	cast(metrics_average_cpc AS double) AS average_cpc,
	cast(metrics_average_time_on_site AS double) AS average_time_on_site,
	cast(metrics_bounce_rate AS double) AS bounce_rate,
	cast(metrics_impressions AS bigint) as impressions,
	cast(metrics_clicks AS bigint) AS clicks,
	cast(metrics_conversions AS double) AS conversions,
	(cast(metrics_cost_micros as double)/1000000) as cost,
    cast(ad_group_criterion_quality_info_quality_score as int) as quality_score,
    cast(ad_group_criterion_quality_info_creative_quality_score as int) as creative_quality_score,
    cast(metrics_historical_quality_score as int) as historical_quality_score,
    cast(metrics_historical_creative_quality_score as int) as historical_creative_quality_score,
    cast(metrics_historical_landing_page_quality_score as int) as historical_landing_page_quality_score,
    cast(metrics_active_view_cpm AS double) as active_view_cpm,
    cast(metrics_active_view_ctr AS double) as active_view_ctr,
    cast(metrics_active_view_impressions AS bigint) as active_view_impressions,
	cast(metrics_active_view_measurability AS double) as active_view_measurability,
	cast(metrics_active_view_measurable_cost_micros AS bigint) as active_view_measurable_cost,
	cast(metrics_active_view_measurable_impressions AS bigint) AS active_view_measurable_impressions,
	cast(metrics_active_view_viewability AS double) AS active_view_view_ability,
    cast(metrics_all_conversions_from_interactions_rate AS double) AS all_conversions_from_interactions_rate,
	cast(metrics_all_conversions_value AS double) AS all_conversion_value,
	cast(metrics_average_cpe AS double) AS average_cpe,
	cast(metrics_average_cpm AS decimal(30,6)) AS average_cpm,
	cast(metrics_average_cpv AS double) AS average_cpv,
	cast(metrics_conversions_from_interactions_rate AS double) AS conversions_from_interactions_rate,
    cast(metrics_conversions_value AS double) AS conversion_value,
    cast(metrics_cost_per_all_conversions AS double) AS cost_per_all_conversions,
	cast(metrics_cost_per_conversion AS double) AS cost_per_conversion,
	cast(metrics_cost_per_current_model_attributed_conversion AS double) AS cost_per_current_model_attributed_conversion,
	cast(metrics_cross_device_conversions AS double) AS cross_device_conversions,
	cast(metrics_ctr as double) as ctr,
	cast(metrics_current_model_attributed_conversions_value AS double) AS current_model_attributed_conversion_value,
    cast(metrics_current_model_attributed_conversions AS double) AS current_model_attributed_conversions,
	cast(metrics_engagement_rate as double) as engagement_rate,
	cast(metrics_engagements AS bigint) AS engagements,
	cast(metrics_gmail_forwards AS int) AS gmail_forwards,
	cast(metrics_gmail_saves AS int) AS gmail_saves,
	cast(metrics_gmail_secondary_clicks as int) as gmail_secondary_clicks,
	metrics_historical_search_predicted_ctr as historical_search_predicted_ctr,
	cast(metrics_interaction_rate as double) as interaction_rate,
    metrics_interaction_event_types as interaction_types,
    cast(metrics_interactions AS bigint) AS interactions,
    cast(metrics_percent_new_visitors as double) as percent_new_visitors,
    cast(metrics_search_absolute_top_impression_share AS double) AS search_absolute_top_impression_share,
	cast(metrics_search_budget_lost_absolute_top_impression_share AS double) AS search_budget_lost_absolute_top_impression_share,
	cast(metrics_search_budget_lost_top_impression_share AS double) AS search_budget_lost_top_impression_share,
	cast(metrics_search_exact_match_impression_share AS double) AS search_exact_match_impression_share,
    cast(metrics_search_impression_share AS double) AS  search_impression_share,
    cast(metrics_search_rank_lost_absolute_top_impression_share AS double) AS search_rank_lost_absolute_top_impression_share,
    cast(metrics_search_rank_lost_impression_share AS double) AS search_rank_lost_impression_share,
    cast(metrics_search_rank_lost_top_impression_share AS double) AS search_rank_lost_top_impression_share,
    cast(metrics_value_per_all_conversions as double) as value_per_all_conversions,
    cast(metrics_value_per_conversion as double) as value_per_conversion,
    cast(metrics_value_per_current_model_attributed_conversion as double) as value_per_current_model_attributed_conversion,
    cast(metrics_video_quartile_p100_rate as double) as video_quartile100_rate,
    cast(metrics_video_quartile_p25_rate as double) as video_quartile25_rate,
    cast(metrics_video_quartile_p50_rate as double) as video_quartile50_rate,
    cast(metrics_video_quartile_p75_rate as double) as video_quartile75_rate,
    cast(metrics_video_view_rate as double) as video_view_rate,
    cast(metrics_video_views AS int) AS video_views,
    cast(metrics_view_through_conversions AS int) AS view_through_conversions,
	segments_date as data_date
FROM raw.googleads_keyword_performance
where ad_group_criterion_keyword_text != 'ad_group_criterion_keyword_text' and data_date='{data_date}';


