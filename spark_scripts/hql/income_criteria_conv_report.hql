alter table raw.googleads_income_criteria_conv_report add if not exists partition (data_date='{data_date}');

WITH api_data AS (
select
segments_ad_network_type as ad_network_type,
cast(ad_group_id AS bigint) as ad_group_id,
segments_conversion_action_category as conversion_category_name,
segments_conversion_action_name as conversion_type_name,
cast(metrics_conversions AS double) AS conversions,
cast(segments_date as date) as report_dt,
segments_device as device,
cast(ad_group_criterion_criterion_id AS bigint) AS id,
ad_group_criterion_type_ as criteria_type,
cast(customer_id AS bigint) as external_customer_id,
segments_date as data_date
FROM  raw.googleads_income_criteria_conv_report
where segments_ad_network_type != 'segments_ad_network_type' and data_date='{data_date}'
),
--Note: Once customer id is deactivated we can't get data for that customer even when customer activated
deactivated_customer_data AS (
    SELECT 
        *
    FROM
        mart_mops.googleads_income_criteria_conv_report
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
INSERT OVERWRITE TABLE mart_mops.googleads_income_criteria_conv_report
PARTITION(data_date)
select
    *
from  
    source_data;
