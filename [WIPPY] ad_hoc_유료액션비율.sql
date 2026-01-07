SELECT 
    
    case when description like '%쪽지%' or description like '%친추%' then '친추'
            when description like '%프까%' or description like '%프로필 오픈%' then '프까' end as use_type
    , case when quantity<0 then -1 else 0 end as quantity_free
    , count(distinct j.id) as cnt_use
    , avg(quantity) as qunt

FROM (select id, user_id,quantity, description from wippy_bronze.billings_jellyuselog
where date_ymd_kst between '2025-12-15' and '2025-12-31') j
join (select id, gender from wippy_dump.accounts_user where gender=0) u on u.id=  j.user_id
and (description like '%쪽지%' or description like '%프까%' or description like '%프로필 오픈%' or description like '%친추%')
and (description not in ('(여)쪽지 발송','오늘 볼래 프까 여자 개수','한일 추천 프로필 오픈'))
group by 1,2
