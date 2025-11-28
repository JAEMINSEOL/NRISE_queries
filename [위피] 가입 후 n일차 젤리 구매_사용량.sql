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
jelly_usage as(select
    case
        when description like '%환불%' and quantity>0  then '환불'
        when description like '%관리자%' then '관리자'
        when description like '%추추%' then '채추추'
        when description like '%프까%' or description like '%프로필 오픈%' then '프까'
        when description like '%리워드%' then '리워드'
        when description like '%팩%' and quantity>0 then '젤리팩'
        when (description like '%특가%' or description like '%할인 적용%') and quantity>0  then '특가구매'
        when description like '%일반%' and quantity>0  then '일반구매'
        when description like '%보상%' and quantity>0  then '보상'
        when description like '%보이스톡%' then '보이스톡'
        when description like '%동놀%' then '동놀'
        when description like '%오늘 볼래%' then '오볼'
        when description like '%채팅%' then '채팅'
        when description like '%쪽지%' then '쪽지'
        when (description like '%시작%' or description like '%평가%' or description like '%리포트%' or description like '%설문%') and description not like '%채팅%' then '설문'
        when description like '%친추%' or description like '%친구 신청%' then '친추'
        when description like '%한일%' then '재팬'
        when description like '%위피패스%' then '위피패스'
        else null end as type
    , case 
        when description like '%결제취소%' then '결제취소'
        when description like '%관리자 지급(지급)%' then '관리자 지급'
        when description like '%관리자 지급(회수)%' then '관리자 회수'
        when description like '%관리자 지급(테스트%' then '관리자 테스트'
        when description like '%환불 젤리%' then '환불'
        when description like '%보상 젤리%' then '신고 보상'
        when description like '%스타터팩%' then '스타터팩'
        when description like '%위피패스 리워드%' then '위피패스'
        when description like '%채팅보상%' then '채팅보상'
        else description end as description
    , user_id,registered_time, quantity, date_ymd_kst,item_id
from wippy_bronze.billings_jellyuselog
where date(registered_time) between date ('2025-08-05') and date('2025-11-25')
    and description not like '%주병규%' and description not like '%김수현%'
order by 1,3)

, user_jelly as(
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
               from (select * from jelly_usage where type not in ('환불','보상','리워드','관리자') and type is not null)
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
        , abs(avg(coalesce(purchase_amount,0))) as daily_arpu
        , abs(avg(coalesce(jelly_outcome,0))) as daily_jelly_outcome
        , avg(nullif(jelly_income,0)) as daily_jelly_income_purchased
        , abs(avg(nullif(jelly_outcome,0))) as daily_jelly_outcome_purchased
        , abs(avg(nullif(purchase_amount,0))) as daily_arppu
    from user_jelly
    group by 1,2
)
select *
    , sum(daily_jelly_income) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_jelly_income
    , sum(daily_jelly_outcome) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_jelly_outcome
    , sum(daily_arpu) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_arpu
    , sum(daily_jelly_income_purchased) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_jelly_income_purchased
    , sum(daily_jelly_outcome_purchased) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_jelly_outcome_purchased
    , sum(daily_arppu) over (partition by user_group order by nth_day rows between unbounded preceding and current row) as cumul_arppu
from s1
order by nth_day, user_group
