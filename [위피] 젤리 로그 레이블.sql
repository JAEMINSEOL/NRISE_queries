select distinct
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
    , case when description like '%결제취소%' then '결제취소'
        when description like '%관리자 지급(지급)%' then '관리자 지급'
        when description like '%관리자 지급(회수)%' then '관리자 회수'
        when description like '%관리자 지급(테스트%' then '관리자 테스트'
        when description like '%환불 젤리%' then '환불'
        when description like '%보상 젤리%' then '신고 보상'
        when description like '%스타터팩%' then '스타터팩'
        when description like '%위피패스 리워드%' then '위피패스'
        when description like '%채팅보상%' then '채팅보상'
        else description end as description
    , avg(quantity) as quantity
from wippy_bronze.billings_jellyuselog
where date(registered_time) between date ('2025-08-05') and date('2025-11-25')
    and description not like '%주병규%' and description not like '%김수현%'
group by 1,2
order by 1,3
