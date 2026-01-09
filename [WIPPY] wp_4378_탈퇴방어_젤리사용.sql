select u.user_id, user_type, response, continue_server_access_time, delete_server_access_time, last_server_access_time
                , refund_quantity
                , case when purchased_amount > 0 then 'purchased' end as jelly_purchase
                , sum(case when quantity>0 then quantity end)-refund_quantity as earned_jelly
                , sum(case when quantity<0 and (description not like '%만료%') then abs(quantity) end) as used_jelly
                , sum(case when (description like '%만료%') and quantity<0 then abs(quantity) end) as expired_jelly
                , sum(case when (description like '%추추%') and quantity<0 then 1 end) as more_rcmd_cnt
                , sum(case when (description like '%프까%') and quantity<0 then 1 end) as profile_open_cnt
                , sum(case when (description like '%쪽지%' or description like '%친추%') and quantity<0 then 1 end) as friend_request_cnt
                , sum(case when (description like '%채팅%') and quantity<0 then 1 end) as chat_cnt
                , array_join(array_agg(distinct description), ',') as agg
                
from da_adhoc.exp_wp_4378 u
join (select quantity, description, registered_time, user_id
        from wippy_bronze.billings_jellyuselog
        where date_ymd_kst >= '2025-12-17'
        -- and quantity<0
        ) j on j.user_id = u.user_id and date_diff('second',u.continue_server_access_time,j.registered_time) > -5

group by 1,2,3,4,5,6,7,8
-- order by u.user_id, j.registered_time
