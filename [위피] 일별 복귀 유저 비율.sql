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
        and first_approval_time between cast('2025-08-05 00:00:00'  as timestamp) - interval '9' hour and cast('2025-11-25 00:00:00' as timestamp) - interval '9' hour
        and (coalesce (ci_hash_user_seq , 1)=1 and coalesce (mobile_hash_user_seq , 1)=1)
    )

, user_jelly as(
    select u.user_group, u.ci_hash, u.date_ymd_kst,u.first_approval_date, u.first_approval_time
        , min(first_event_time) as first_event_time
        , array_join(array_agg(cast(u.user_id as varchar)), ',') as user_ids
        , count(distinct au.user_id) as user_activate
        , sum(purchase_amount) as purchase_amount, sum(jelly_income) as jelly_income
        , min(pb.first_purchase_time) as first_purchase_time
    from (select *
            from users
            cross join (select distinct date_ymd_kst
                        from wippy_bronze.billings_jellyuselog
                        where date(date_ymd_kst) between date('2025-08-05') and date('2025-11-25'))
            where date (first_approval_time + interval '9' hour) <= date(date_ymd_kst)) u
    left join (select user_id, registered_time, first_event_time, last_event_time
                from wippy_silver.daily_active_user_info) au on au.user_id = u.user_id and date(au.first_event_time + interval '9' hour) = date(u.date_ymd_kst)
    left join (select user_id, date_ymd_kst, sum(sales_amount) as purchase_amount, sum(jelly_quantity) as jelly_income
               from wippy_silver.daily_billing
               group by user_id, date_ymd_kst) ji on ji.user_id = u.user_id and date(ji.date_ymd_kst)=date(u.date_ymd_kst)
    left join (select user_id, min(registered_time) as first_purchase_time
                from wippy_bronze.billing_log
                group by user_id) pb on pb.user_id = u.user_id

    where ci_hash is not null
    group by 1,2,3,4,5
    order by user_ids
    )
--                                                          ,
, s1 as(
select *
        , case when EXTRACT(DAY FROM cast(first_event_time as timestamp) - cast(prev_event_time as timestamp)) >= 15 then 1 else 0 end as return_idx
from (select *
        , lag(first_event_time) over (partition by ci_hash order by first_event_time) as prev_event_time
        from user_jelly)
        )
select *, cumul_returned_users *100.0 / cumul_users as cumul_return_rate
from (select *
           , num_return_users * 100.0 / num_users as return_rate
           , sum(num_return_users)                          over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_returned_users
        , sum(num_users) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_users
      from (select user_group
                 , date_diff('day', first_approval_date, date(date_ymd_kst)) + 1 as nth_day
                 , count(distinct ci_hash)                                       as num_users
                 , sum(return_idx)                                               as num_return_users
            from s1
            group by 1, 2))

