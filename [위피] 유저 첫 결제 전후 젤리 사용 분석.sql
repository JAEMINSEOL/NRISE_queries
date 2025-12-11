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
        and first_approval_time between cast('2025-08-05 00:00:00'  as timestamp) - interval '9' hour and cast('2025-11-08 00:00:00' as timestamp) - interval '9' hour
        and (coalesce (ci_hash_user_seq , 1)=1 and coalesce (mobile_hash_user_seq , 1)=1)
    )
    , daily_active_users as
    (select user_id, date_ymd_kst
        from wippy_silver.daily_active_user_info
        where first_approval_time between cast('2025-08-05 00:00:00'  as timestamp) - interval '9' hour and cast('2025-11-08 00:00:00' as timestamp) - interval '9' hour)
, jelly_use as(
    select user_group
            , first_approval_date
            , date_diff('day',first_approval_date,date(date_ymd_kst))+1 as nth_day
             , coalesce(max(case when price>0 then 1 end) over (partition by ci_hash),0) as has_purchased
            , row_number() over (partition by ci_hash order by ju.registered_time) as action_num
            , case when jb.registered_time is not null then
                    row_number() over (partition by ci_hash, case when jb.registered_time is not null then 1 else 0 end
                                        order by jb.registered_time)
                    else null end as purchase_num
            , coalesce(max(case when item_id = 2088 then 1 end) over (partition by ci_hash),0) as get_referral_reward
             ,ci_hash, ju.user_id, date_ymd_kst, (quantity) as quantity, item_id,jb.price
             ,case when description like '채팅보상%' then '채팅보상'
                    when description like '신고번호%' then '신고보상'
                    when description like '%동놀 추가%' and description like '%환불%' then '동놀 환불'
                    else description end as description
            , coalesce(sum((case when quantity<0 then abs(quantity) end)) over (partition by ju.user_id order by ju.registered_time),0) as cumul_jelly_use
    from wippy_bronze.billings_jellyuselog ju
    join (select user_group,ci_hash, user_id, first_approval_date from users) u on ju.user_id = u.user_id
    left join wippy_bronze.billing_log jb on jb.user_id = u.user_id and jb.jelly_use_log_id = ju.id
    where description not like '%김수현%' and description not like '%주병규%' and description not like '%전지원%'
)
,
first_purchase_exp as (select * from jelly_use
                        where purchase_num = 1)
,summary as(
select u.relative_score,j.*
        , case when j.description like '%한일%' then '한일'
                when j.description like '%채추추%' or j.description like '%추추%' or j.description like '%추가 추천%' then '추추'
                when j.description like '%프까%' or j.description like '%프로필 오픈%' then '프까'
                when j.description like '%채팅%' and j.description not like '%DM%' then '채팅'
                when j.description like '%프로필 평가%' or j.description like '%닮은꼴%' or j.description like '%리포트%' then '리포트'
                when j.description like '%보이스톡%'  then '보이스톡'
                when j.description like '%쪽지%' or j.description like '%DM%' or j.description like '%친추%' then '친구 신청'
                when j.description like '%타임어택%' then '친구 신청'
                when j.description like '%블라인드%' then '블라인드'
                when j.description like '%동놀%' or j.description like '%오늘 볼래%' then '동네약속'
        else null end as use_type
        , da.date_ymd_kst as active_date
    from jelly_use j
    left join first_purchase_exp p on p.ci_hash = j.ci_hash
    left join daily_active_users da on da.user_id = j.user_id and date(da.date_ymd_kst) = date(j.date_ymd_kst)
    join (select id, relative_score from wippy_dump.accounts_user) u on u.id = j.user_id
where (j.action_num <= p.action_num + 20 or p.action_num is null)
and j.nth_day <= 30

                                                         )

-- select user_group, get_referral_reward, has_purchased, ci_hash, first_approval_date
--         , max(nth_day) as last_jellyuse_date
--         , coalesce(sum(price),0) as purchase_amount
--         , count(distinct active_date) as active_days
-- from summary
-- group by 1,2,3,4,5

select * from summary
-- where ci_hash = '360ff04d665a73ecb22345ffbc78a5e34f258da79d762100379756f99248b0b3'
-- group by 1

-- group by 2,3,4


-- select user_group, purchased_group, referral_rewarded_group
--         , count(distinct ci_hash) as num_users
--         , avg(used_jellies) as used_jellies
--         , avg(purchase_amount) as purchase_amount
-- from user_14d
-- group by 1,2,3
-- order by 1,2,3

