with users as
    (select u.*, t.source_type
        , date (first_approval_time + interval '9' hour) as first_approval_date
       , case when source_type is null then 'A' else 'B' end as user_group
    from wippy_silver.user_activation_metrics u
    left join (select user_id, source_type
                    from wippy_dump.reward_grant
                    where source_type ='MALE_NEW_USER_FIRST_JOIN_V1') t
        on t.user_id = u.user_id
    where gender=0
        and date (first_approval_time + interval '9' hour) between date ('2025-08-05') and date('2025-08-19')
        and (coalesce (ci_hash_user_seq , 1)=1 and coalesce (mobile_hash_user_seq , 1)=1)
    )

, user_jelly as(
    select *
        , sum(purchase_amount) over (partition by ci_hash order by date_ymd_kst rows between unbounded preceding and current row) as cumul_purchase_amount
        from(
    select u.user_group, u.ci_hash, u.date_ymd_kst,u.first_approval_date
        -- , array_join(array_agg(cast(u.user_id as varchar)), ',') as user_ids
        , count(distinct au.user_id) as user_activate
        , sum(purchase_amount) as purchase_amount, sum(jelly_income) as jelly_income
    from (select *
            from users
            cross join (select distinct date_ymd_kst
                        from wippy_bronze.billings_jellyuselog
                        where date(date_ymd_kst) between date('2025-08-05') and date('2025-12-25'))
            where date (first_approval_time + interval '9' hour) <= date(date_ymd_kst)) u
    left join (select user_id, registered_time, first_event_time, last_event_time
                from wippy_silver.daily_active_user_info) au on au.user_id = u.user_id and date(au.first_event_time + interval '9' hour) = date(u.date_ymd_kst)
    left join (select user_id, date_ymd_kst, sum(sales_amount) as purchase_amount, sum(jelly_quantity) as jelly_income
               from wippy_silver.daily_billing
               group by user_id, date_ymd_kst) ji on ji.user_id = u.user_id and date(ji.date_ymd_kst)=date(u.date_ymd_kst)

    where ci_hash is not null
    group by 1,2,3,4
    -- order by user_ids
    )
    )
--     select * from user_jelly
--     order by ci_hash, date_ymd_kst
, s1 as (
    select user_group
        --  , first_approval_date
         , nth_day
         , count(distinct ci_hash) as num_users
         , sum(cumul_purchase_amount) as cumul_purchase_amount
         , sum(case when user_activate > 0  then 1 end) as activated_users
        , avg(coalesce(jelly_income,0)) as daily_jelly_income
        , abs(avg(coalesce(purchase_amount,0))) as daily_arpu
        , avg(nullif(jelly_income,0)) as daily_jelly_income_purchased
        , abs(avg(nullif(purchase_amount,0))) as daily_arppu
        , count(distinct case when purchase_amount>0 then ci_hash end) as num_purchase_users
    from (select *, date_diff('day',first_approval_date,date(date_ymd_kst))+1 as nth_day from user_jelly)
    group by 1,2
)
select *
     , activated_users *100.0 / num_users as dau_rate
    , sum(daily_jelly_income) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_jelly_income
    , cumul_purchase_amount / num_users as cumul_arpu
    , sum(daily_jelly_income_purchased) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_jelly_income_purchased
    , sum(daily_arppu) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_arppu
    , sum(num_purchase_users) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_num_purchase_users
from s1
where nth_day<=111
order by user_group
