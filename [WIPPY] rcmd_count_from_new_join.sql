
select user2_gender as gender
        -- , hour_kst
        , relative_score_grade
        , min*5 as min
        , avg(rcmd_users) as rcmd_users
        , avg(cumul_rcmd_users) as cumul_rcmd_users
        , avg(cumul_rcmd_response_users) as cumul_rcmd_response_users
        , avg(cumul_rcmd_request_users) as cumul_rcmd_request_users
        , avg(cumul_rcmd_2way_response_users) as cumul_rcmd_2way_response_users
        , avg(cumul_rcmd_2way_response_users) *100.0 / avg(cumul_rcmd_response_users) as rcmd_2way_response_rate
        , count(distinct user2_id) as user2_cnt
from (select *
           , sum(rcmd_users) over (partition by user2_id order by min rows between unbounded preceding and current row) as cumul_rcmd_users
           , sum(rcmd_response_users) over (partition by user2_id order by min rows between unbounded preceding and current row) as cumul_rcmd_response_users
           , sum(rcmd_request_users) over (partition by user2_id order by min rows between unbounded preceding and current row) as cumul_rcmd_request_users
           , sum(rcmd_2way_response_users) over (partition by user2_id order by min rows between unbounded preceding and current row) as cumul_rcmd_2way_response_users
      from (select rc.user2_id
                , floor(rc.user2_relative_score/30) +1 as relative_score_grade
                , rc.user2_gender
                , rc.date_ymd_kst
                , rc.hour_kst
                , floor(date_diff('minute', cast(u.approval_time as timestamp), cast(rc.registered_time as timestamp))/5) as min
                , coalesce(count(distinct user1_id),0) as rcmd_users
                , coalesce(count(distinct case when rc.like_time is not null then user1_id end),0) as rcmd_response_users
                , coalesce(count(distinct case when rr.like_time is not null or rr.dislike_time is not null then user1_id end),0) as rcmd_2way_response_users
                , coalesce(count(distinct case when rc.friend_request_time is not null then user1_id end),0) as rcmd_request_users
                -- , coalesce(count(distinct case when ch.other_accepted_time is not null or ch.other_rejected_time is not null then user1_id end),0) as rcmd_2way_response_users
            from
                (select * from wippy_silver.hourly_merged_rcmd_log
                    where date_ymd_kst >= '2025-12-01') rc
                join
                (select * from wippy_dump.accounts_user
                    -- where date(first_approval_time + interval '9' hour) between date('2025-12-01') and date('2025-12-20')
                    where first_approval_time >= cast('2025-12-01' as timestamp) - interval '9' hour) u
                    on rc.user2_id = u.id and date (rc.date_ymd_kst) <= date_add('day',1, date (u.date_ymd_snapshot))
                left join
                (select user1_id as user_id, user2_id, registered_time, like_time, dislike_time from wippy_silver.hourly_merged_rcmd_log
                    where date_ymd_kst >= '2025-12-01') rr
                    on rr.user_id = rc.user2_id and rr.user2_id = rc.user1_id and rr.registered_time >= rc.registered_time
                left join
                (select * from wippy_dump.chats_chatroom
                    where registered_time >= cast('2025-12-01' as timestamp) - interval '9' hour) ch 
                    on (ch.owner_id = rc.user1_id and ch.invited_user_id = rc.user2_id) and ch.registered_time >= rc.registered_time
            
            group by 1, 2, 3, 4, 5,6)
            where min*5 <= 60*24 and min >= 0
            )
where user2_gender=1
    and relative_score_grade <=10
group by 1,2,3
order by 1,2,3
