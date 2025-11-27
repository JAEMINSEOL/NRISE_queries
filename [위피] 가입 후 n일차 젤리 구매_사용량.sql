with users as
    (select u.*, t.source_type
        , date (first_approval_time) as first_approval_date
       , case when source_type is null then 'A' else 'B' end as user_group
    from wippy_silver.user_activation_metrics u
    left join (select user_id, source_type
                    from wippy_dump.reward_grant
                    where source_type ='MALE_NEW_USER_FIRST_JOIN_V1') t
        on t.user_id = u.user_id
    where gender=0
        and date (first_approval_time + interval '9' hour) between date ('2025-08-05') and date('2025-11-25')
        and (coalesce (ci_hash_user_seq , 1)=1 and coalesce (mobile_hash_user_seq , 1)=1)
    )

,
user_jelly as(
    select u.user_group, u.ci_hash, u.date_ymd_kst,u.first_approval_date
        , array_join(array_agg(cast(u.user_id as varchar)), ',') as user_ids
        , sum(purchase_amount) as purchase_amount, sum(jelly_income) as jelly_income, sum(jelly_outcome) as jelly_outcome
        , sum(num_matching) as num_matching
    from (select *
            from users
            cross join (select distinct date_ymd_kst
                        from wippy_bronze.billings_jellyuselog
                        where date(date_ymd_kst) between date('2025-08-05') and date('2025-11-25'))
            where date (first_approval_time + interval '9' hour) <= date(date_ymd_kst)) u
    left join (select user_id, date_ymd_kst, sum(sales_amount) as purchase_amount, sum(jelly_quantity) as jelly_income
               from wippy_silver.daily_billing
               group by user_id, date_ymd_kst) ji on ji.user_id = u.user_id and date(ji.date_ymd_kst)=date(u.date_ymd_kst)
    left join (select user_id, date_ymd_kst,sum(quantity) as jelly_outcome
                    , count(case when item_id in (11,13) then user_id end) as num_matching
               from wippy_bronze.billings_jellyuselog
                where quantity < 0
               group by user_id,date_ymd_kst) jo on jo.user_id = u.user_id and date(jo.date_ymd_kst)=date(u.date_ymd_kst)
    where ci_hash is not null
    group by 1,2,3,4
    order by user_ids
    )
, s1 as (
    select user_group
    --      , first_approval_date, date_ymd_kst
         , date_diff('day',first_approval_date,date(date_ymd_kst))+1 as nth_day
        , avg(coalesce(jelly_income,0)) as daily_jelly_income
        , sum(coalesce(jelly_income,0)) as daily_jelly_income_all
        , abs(avg(coalesce(jelly_outcome,0))) as daily_jelly_outcome
    from user_jelly
    group by 1,2
)
select *
    , sum(daily_jelly_income) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_jelly_income
    , sum(daily_jelly_outcome) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_jelly_outcome
    , sum(daily_jelly_income_all) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_jelly_income_all
from s1
order by nth_day, user_group
