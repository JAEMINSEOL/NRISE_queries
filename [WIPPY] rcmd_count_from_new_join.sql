select *
           , sum(rcmd_users) over (partition by user2_id order by min_rc rows between unbounded preceding and current row) as cumul_rcmd_users
           , sum(rcmd_response_users) over (partition by user2_id order by min_rc rows between unbounded preceding and current row) as cumul_rcmd_response_users
           , sum(rcmd_request_users) over (partition by user2_id order by min_rc rows between unbounded preceding and current row) as cumul_rcmd_request_users
           , sum(rcmd_2way_impression_users) over (partition by user2_id order by min_rc rows between unbounded preceding and current row) as cumul_rcmd_2way_impression_users
           , sum(rcmd_2way_response_users) over (partition by user2_id order by min_rc rows between unbounded preceding and current row) as cumul_rcmd_2way_response_users
      from (select rc.user2_id
                , rc.user2_relative_score
                , floor(rc.user2_relative_score/30) +1 as relative_score_grade
                , rc.user2_gender
                , rc.date_ymd_kst
                , rc.hour_kst
                , substr(cast(u.location as varchar),1,3) as user2_location
                , floor(date_diff('minute', cast(u.approval_time as timestamp), cast(rc.registered_time as timestamp))/5) as min_rc
--           , floor(date_diff('minute', cast(u.approval_time as timestamp), cast(rr.impression_time as timestamp))/5) as min_rr
--                 , floor(date_diff('minute', cast(u.approval_time as timestamp), cast(greatest(coalesce(rr.like_time,cast('2000-01-01' as timestamp)),coalesce(rr.dislike_time,cast('2000-01-01' as timestamp))) as timestamp))/5) as min_rr
                , coalesce(count(distinct user1_id),0) as rcmd_users
                , coalesce(count(distinct case when rc.like_time is not null then user1_id end),0) as rcmd_response_users
                , coalesce(count(distinct case when rc.like_time is not null and (rr.like_time is not null or rr.dislike_time is not null) then user1_id end),0) as rcmd_2way_response_users
                 , coalesce(count(distinct case when rc.like_time is not null and (rr.impression_time is not null) then user1_id end),0) as rcmd_2way_impression_users
                , coalesce(count(distinct case when rc.friend_request_time is not null then user1_id end),0) as rcmd_request_users
                -- , coalesce(count(distinct case when ch.other_accepted_time is not null or ch.other_rejected_time is not null then user1_id end),0) as rcmd_2way_response_users
      from
                (select* from (select *,
                                      row_number() over (partition by user1_id, user2_id order by registered_time) as rn
                               from wippy_silver.hourly_merged_rcmd_log
                               where date_ymd_kst >= '2025-12-01'
                                 and user1_gender = 0
                                 ) rc
                         where rn=1
--                            and rcmd_type <> 50
                    ) rc
                join
                (select id, gender, location, approval_time  from wippy_dump.accounts_user
                    -- where date(first_approval_time + interval '9' hour) between date('2025-12-01') and date('2025-12-20')
                    where first_approval_time >= cast('2025-12-01' as timestamp) - interval '9' hour
                   ) u
                    on rc.user2_id = u.id
                left join
                (select user1_id as user_id, user2_id, max(impression_time) as impression_time, max(registered_time) as registered_time, max(like_time) as like_time, max(dislike_time) as dislike_time from wippy_silver.hourly_merged_rcmd_log
                    where date_ymd_kst >= '2025-12-01'
                        and rcmd_type=3 and channel_id=3 and user1_gender=1
                    group by 1,2) rr
                    on rr.user_id = rc.user2_id and rr.user2_id = rc.user1_id and rr.registered_time >= rc.registered_time
                left join
                (select owner_id, invited_user_id, registered_time from wippy_dump.chats_chatroom
                    where registered_time >= cast('2025-12-01' as timestamp) - interval '9' hour) ch
                    on (ch.owner_id = rc.user1_id and ch.invited_user_id = rc.user2_id) and ch.registered_time >= rc.registered_time
            where rc.user2_relative_score<100
            group by 1, 2, 3, 4, 5, 6, 7,8)
            where min_rc*5 <= 60*24 and min_rc >= 0
