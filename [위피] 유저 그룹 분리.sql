with users as
    (select u.ci_hash, u.user_id,u.first_approval_time, t.source_type
        , date (first_approval_time + interval '9' hour) as first_approval_date
       , case when source_type is null then 'A' else 'B' end as user_group
    from wippy_silver.user_activation_metrics u
    left join (select user_id, source_type
                    from wippy_dump.reward_grant
                    where source_type ='MALE_NEW_USER_FIRST_JOIN_V1') t
        on t.user_id = u.user_id
    where gender=0
        and first_approval_time between cast('2025-08-05 00:00:00'  as timestamp) - interval '9' hour and cast('2025-11-15 00:00:00' as timestamp) - interval '9' hour
        and (coalesce (ci_hash_user_seq , 1)=1 and coalesce (mobile_hash_user_seq , 1)=1)
    )
, jelly_use as(
    select user_id, date_ymd_kst, abs(quantity) as quantity, description
            , sum(abs(quantity)) over (partition by user_id order by registered_time) as cumul_jelly_use
    from wippy_bronze.billings_jellyuselog
    where quantity<0
    and date(date_ymd_kst) between date ('2025-08-04') and date('2025-12-25'))
, user_daily_jelly as(
    select u.user_group, u.ci_hash,u.first_approval_date, u.first_approval_time, u.date_ymd_kst, u.nth_day
        , array_join(array_agg(distinct cast(u.user_id as varchar)), '..') as user_ids
        , coalesce(sum(ji.jelly_use),0) as jelly_outcome
        , sum(purchase_amount) as purchase_amount
        , max(case when p.user_id is not null then 1 else 0 end) as has_purchased
        , max(get_referral_reward) as get_referral_reward
        , max(chat_exp) as chat_exp
        , max(chat_exp_within_50) as chat_exp_within_50
    from (select *, date_diff('day',first_approval_date,date(date_ymd_kst))+1 as nth_day
            from users
            cross join (select distinct date_ymd_kst
                        from wippy_bronze.billings_jellyuselog
                        where date(date_ymd_kst) between date('2025-08-05') and date('2025-11-25'))
            where date (first_approval_time + interval '9' hour) <= date(date_ymd_kst)) u
    left join (select user_id, date_ymd_kst, sum(abs(quantity)) as jelly_use
                    , count(distinct case when description like '%채팅%' and description not like '%환불%' and description not like '%보상%' then 1 end) as chat_exp
                    , count(distinct case when description like '%채팅%' and description not like '%환불%' and description not like '%보상%' and cumul_jelly_use <= 50 then 1 end) as chat_exp_within_50
               from jelly_use
               group by user_id, date_ymd_kst) ji on ji.user_id = u.user_id and date(ji.date_ymd_kst) = date(u.date_ymd_kst)
    left join (select user_id, date_ymd_kst
                    , max(case when item_id=2088 then 1 else 0 end) as get_referral_reward
                from wippy_bronze.billings_jellyuselog
                where date(date_ymd_kst) between date ('2025-08-04') and date('2025-12-25')
               group by user_id, date_ymd_kst) ja on ja.user_id = u.user_id and date(ja.date_ymd_kst) = date(u.date_ymd_kst)
    left join (select distinct user_id, date_ymd_kst, sum(sales_amount) as purchase_amount
                from wippy_silver.daily_billing
                where date(date_ymd_kst) between date ('2025-08-04') and date('2025-12-25')
                group by 1,2) p on p.user_id = u.user_id and date(p.date_ymd_kst) = date(u.date_ymd_kst)
    group by 1,2,3,4,5,6
    order by user_group, has_purchased
    )

,user_nth as(
select udj.user_group, udj.ci_hash, nth_day
        -- , count(distinct udj.ci_hash) as num_users
        , avg(jelly_outcome) as used_jellies
        , avg(coalesce(purchase_amount,0)) as purchase_amount
from user_daily_jelly udj
left join (
    select user_group, ci_hash
            , coalesce(max(has_purchased),0) as purchased_group
            , coalesce(max(get_referral_reward),0) as referral_rewarded_group
    from user_daily_jelly
    group by 1,2
) u on u.ci_hash = udj.ci_hash
where referral_rewarded_group = 0
group by 1,2,3
)
select * from user_nth
-- select user_group, nth_day, num_users
--     , sum(purchase_amount) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_arpu
--     , sum( used_jellies) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_jelly
--     from user_nth

-- , user_14d as(
-- select user_group, ci_hash
--         , coalesce(max(has_purchased),0) as purchased_group
--         , coalesce(max(get_referral_reward),0) as referral_rewarded_group
--         , sum(jelly_outcome) as used_jellies
--         , coalesce(sum(purchase_amount),0) as purchase_amount
-- from user_daily_jelly
-- where nth_day <=14
-- group by 1,2
-- order by ci_hash
-- )

-- select * from user_14d

-- select user_group, purchased_group, referral_rewarded_group
--         , count(distinct ci_hash) as num_users
--         , avg(used_jellies) as used_jellies
--         , avg(purchase_amount) as purchase_amount
-- from user_14d
-- group by 1,2,3
-- order by 1,2,3

