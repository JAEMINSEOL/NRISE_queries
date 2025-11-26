with base as (select
        date (first_approval_time) as first_approval_date
       , count (distinct u.user_id) as num_users
       , count (distinct case when coalesce (mobile_hash_user_seq , 0)=1 then u.user_id end) as num_mobile_first_users
       , count (distinct case when coalesce (ci_hash_user_seq , 0)=1 then u.user_id end) as num_ci_first_users
       , count (distinct case when coalesce (ci_hash_user_seq , 0)=1 and not coalesce (mobile_hash_user_seq , 0)=1 then u.user_id end) as num_ci_not_mobile_first_users
       , count (distinct case when not coalesce (ci_hash_user_seq , 0)=1 and coalesce (mobile_hash_user_seq , 0)=1 then u.user_id end) as num_mobile_not_ci_first_users
       , count (distinct case when source_type ='MALE_NEW_USER_FIRST_JOIN_V1' then u.user_id end) as num_actual_rewarded_users
       , count (distinct case when coalesce (ci_hash_user_seq , 0)=1 and not coalesce (mobile_hash_user_seq , 0)=1 and source_type ='MALE_NEW_USER_FIRST_JOIN_V1' then u.user_id end) as num_rewarded_not_analyzed_users
    from wippy_silver.user_activation_metrics u
        left join (select user_id, source_type
                    from wippy_dump.reward_grant
                    where source_type ='MALE_NEW_USER_FIRST_JOIN_V1') t
        on t.user_id = u.user_id
    where gender=0
        and date (first_approval_time) between date ('2025-06-01') and date ('2025-07-01')
    group by 1
    order by 1
    )

select *
from base
