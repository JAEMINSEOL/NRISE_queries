with ubl_part as (SELECT user_id,date_ymd_kst,navigations,server_access_time,event_type,event_props, extra
    FROM wippy_bronze.wippy_ubl
    where date_ymd_kst >= '2025-12-18')

select *
from(
select coalesce(date(c.server_access_time),date(d.server_access_time)) as date_ymd_kst, v.user_id, json_extract_scalar(v.extra, '$.reward_type') as user_type
        , case when c.server_access_time is not null then 'continue' when d.server_access_time is not null then 'delete' end as response
        ,c.server_access_time as continue_server_access_time
        ,d.server_access_time as delete_server_access_time
        ,r.server_access_time as last_server_access_time
        , least(date_diff('hour',coalesce(c.server_access_time,d.server_access_time),coalesce(d.server_access_time,cast('2030-12-29 8:06:17' as timestamp))),1000) as retention_max
        , j.registered_time as jelly_purchase_time
        , date_diff('hour',coalesce(c.server_access_time,d.server_access_time),j.registered_time) as purchased_hour_after_continue
        , price as purchased_amount
from(select * from (SELECT user_id, date_ymd_kst, navigations, event_type, extra
                    ,row_number() over (partition by user_id order by server_access_time) as rn
                    FROM ubl_part
                    where contains(navigations, 'user_delete_reward')
                      and event_type in ('view_content')
                      and json_extract_scalar(extra, '$.reward_type') in ('MALE_HAS_BILLING', 'MALE_TOP_30')
                      and user_id is not null
                    ) where rn=1
    ) v
left join(select * from(
    SELECT user_id,date_ymd_kst,navigations,server_access_time,event_type,event_props['object_value'] as response
            ,row_number() over (partition by user_id order by server_access_time) as rn
    FROM ubl_part
    where contains(navigations,'user_delete_reward')
    and event_type  = 'click'
    and event_props['object_value'] = 'continue'
    ) where rn=1
    ) c on v.user_id = c.user_id
left join(select * from(
    SELECT user_id,date_ymd_kst,navigations,server_access_time,event_type,event_props['object_value'] as response
            ,row_number() over (partition by user_id order by server_access_time desc) as rn
    FROM ubl_part
    where contains(navigations,'user_delete_reward')
    and event_type  = 'click'
    and event_props['object_value'] = 'delete'
    ) where rn=1
    ) d on v.user_id = d.user_id
join(select * from(
    select user_id, date_ymd_kst, server_access_time
            , row_number() over (partition by user_id order by server_access_time desc) as rn
    from ubl_part
    ) where rn=1
    ) r on r.user_id = v.user_id
left join (select * from(
    select user_id, registered_time, price
            , row_number() over (partition by user_id order by registered_time desc) as rn
    from wippy_bronze.billing_log
    where registered_month >= '2025-12'
    ) 
--                     where rn=1
    ) j on j.user_id = v.user_id and date_diff('second',c.server_access_time,j.registered_time) > 0
-- where c.server_access_time is null
order by 3,4
)
where response is not null
