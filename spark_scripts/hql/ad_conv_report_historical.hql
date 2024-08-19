alter table raw.googleads_ad_conv_report add if not exists partition (data_date='{data_date}');
INSERT OVERWRITE TABLE mart_mef.googleads_ad_conv_report
PARTITION(data_date)
select
       segments_ad_network_type as ad_network_type,
       cast(ad_group_id AS bigint) AS ad_group_id,
       segments_conversion_action_category as conversion_category_name,
       segments_conversion_action_name  as conversion_type_name,
       cast(metrics_conversions AS double) AS conversions,
       segments_device as device,
       cast(segments_date as date) as report_dt,
       cast(ad_group_ad_ad_id AS bigint) as id,
       segments_date as data_date
FROM raw.googleads_ad_conv_report
where segments_ad_network_type != 'segments_ad_network_type' and data_date='{data_date}';
