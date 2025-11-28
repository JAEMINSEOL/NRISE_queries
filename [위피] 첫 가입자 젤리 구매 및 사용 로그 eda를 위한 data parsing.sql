with users_first as(
    select distinct u0.ci_hash, u1.user_id
        , max(u0.user_group) over (partition by u0.ci_hash) as user_group
        , min(u1.first_approval_time) over (partition by u0.ci_hash) as first_approval_time
        , count(u1.user_id) over (partition by u0.ci_hash) as user_ids
        , date(u0.first_approval_time) as first_approval_date
    from
    (select u.user_id, u.ci_hash, t.source_type
       , case when source_type is null then 'A' else 'B' end as user_group
       , first_approval_time
    from wippy_silver.user_activation_metrics u
    left join (select user_id, source_type
                    from wippy_dump.reward_grant
                    where source_type ='MALE_NEW_USER_FIRST_JOIN_V1') t
        on t.user_id = u.user_id
    where gender=0
        and date (first_approval_time + interval '9' hour) between date ('2025-08-05') and date('2025-11-25')
        and (coalesce (ci_hash_user_seq , 1)=1 and coalesce (mobile_hash_user_seq , 1)=1)
    ) u0
    join wippy_silver.user_activation_metrics u1 on u0.ci_hash = u1.ci_hash
    order by 5 desc
    )
, jelly_usage as(select
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

, jelly_dec as (
    select u.ci_hash
        -- , u.user_id 
        , first_approval_time
        , registered_time
        , description
        , type
        , abs(quantity) as quantity
    from users_first u
        left join (select * from jelly_usage where type not in ('환불','보상','리워드','관리자') and type is not null) j on j.user_id=u.user_id
    where date(date_ymd_kst) between date ('2025-08-05') and date('2025-11-25')
        and quantity < 0 
        )

, jelly_inc as (
    select u.ci_hash
        -- , u.user_id 
        , first_approval_time
        , registered_time
        , description
        , type
        , quantity
    from users_first u
        left join (select * from jelly_usage where type not in ('환불','보상','리워드','관리자') and type is not null) j on j.user_id=u.user_id
    where date(date_ymd_kst) between date ('2025-08-05') and date('2025-11-25')
        and quantity > 0 
        )     

select * from jelly_dec
