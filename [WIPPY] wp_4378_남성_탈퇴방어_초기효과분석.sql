select date_ymd_kst, user_type, response
        , count(distinct user_id) as users
        from(
select v.date_ymd_kst, v.user_id, json_extract_scalar(v.extra, '$.reward_type') as user_type, response
from(
    SELECT user_id,date_ymd_kst,navigations,event_type,extra
    FROM wippy_bronze.wippy_ubl
    where date_ymd_kst >= '2025-12-18'
    and contains(navigations,'user_delete_reward')
    and event_type in ('view_content')
    and json_extract_scalar(extra, '$.reward_type') in ('MALE_HAS_BILLING','MALE_TOP_30')
    and user_id is not null
) v
join(select * from(
    SELECT user_id,date_ymd_kst,navigations,event_type,event_props['object_value'] as response
            ,row_number() over (partition by user_id order by server_access_time) as rn
    FROM wippy_bronze.wippy_ubl
    where date_ymd_kst >= '2025-12-18'
    and contains(navigations,'user_delete_reward')
    and event_type  = 'click'
    ) where rn=1
-- and json_extract_scalar(extra, '$.reward_type') in ('MALE_HAS_BILLING','MALE_TOP_30')
) c on v.user_id = c.user_id and v.date_ymd_kst = c.date_ymd_kst
)
where response is not null
group by 1,2,3
order by 1,2,3
