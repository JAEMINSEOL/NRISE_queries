WITH numbers AS (
    SELECT minute_offset
    FROM UNNEST(sequence(0, 1440)) AS t(minute_offset)
)

select *
           , sum(rcmd_users) over (partition by user_id order by min_rc rows between unbounded preceding and current row) as cumul_rcmd_users
           , sum(rcmd_impression_users) over (partition by user_id order by min_rc rows between unbounded preceding and current row) as cumul_rcmd_impression_users
           , sum(rcmd_response_users) over (partition by user_id order by min_rc rows between unbounded preceding and current row) as cumul_rcmd_response_users
           , sum(rcmd_request_users) over (partition by user_id order by min_rc rows between unbounded preceding and current row) as cumul_rcmd_request_users

            , sum(rcmd_users_rcmded) over (partition by user_id order by min_rc rows between unbounded preceding and current row) as cumul_rcmd_users_rcmded
            , sum(rcmd_impression_users_rcmded) over (partition by user_id order by min_rc rows between unbounded preceding and current row) as cumul_rcmd_impression_users_rcmded
           , sum(rcmd_response_users_rcmded) over (partition by user_id order by min_rc rows between unbounded preceding and current row) as cumul_rcmd_response_users_rcmded
           , sum(rcmd_request_users_rcmded) over (partition by user_id order by min_rc rows between unbounded preceding and current row) as cumul_rcmd_request_users_rcmded
    from (select u.id as user_id, minute_offset as min_rc, relative_score, date_ymd_kst, hour_kst, user2_location
          , coalesce(rcmd_users,0) as rcmd_users, coalesce(rcmd_impression_users,0) as rcmd_impression_users
          , coalesce(rcmd_response_users,0) as rcmd_response_users, coalesce(rcmd_request_users,0) as rcmd_request_users
          , coalesce(rcmd_users_rcmded,0) as rcmd_users_rcmded, coalesce(rcmd_impression_users_rcmded,0) as rcmd_impression_users_rcmded
          , coalesce(rcmd_response_users_rcmded,0) as rcmd_response_users_rcmded, coalesce(rcmd_request_users_rcmded,0) as rcmd_request_users_rcmded

            from (select id,relative_score, minute_offset
                      from (select id,relative_score from wippy_dump.accounts_user
                            where date(first_approval_time + interval '9' hour) between date('2025-12-01') and date('2025-12-28')
                            and gender=1
                           ) u
                    cross join numbers) u
            left join (
                        select rc.user1_id as user_id
                        , rc.user1_relative_score
                        , rc.date_ymd_kst
                        , rc.hour_kst
                         , floor(date_diff('minute', cast(u.approval_time as timestamp), cast(rc.impression_time as timestamp))/5) as min_rc
        --                 , floor(date_diff('minute', cast(u.approval_time as timestamp), cast(greatest(coalesce(rc.like_time,cast('2000-01-01' as timestamp)),coalesce(rc.dislike_time,cast('2000-01-01' as timestamp))) as timestamp))/5) as min_rc
                        , substr(cast(u.location as varchar),1,3) as user2_location
                        , coalesce(count(distinct user2_id),0) as rcmd_users
                         , coalesce(count(distinct case when rc.impression_time is not null then user2_id end),0) as rcmd_impression_users
                        , coalesce(count(distinct case when (rc.like_time is not null or rc.dislike_time is not null) then user2_id end),0) as rcmd_response_users
                        , coalesce(count(distinct case when rc.friend_request_time is not null then user2_id end),0) as rcmd_request_users

                        , coalesce(count(distinct case when (rcmd_type=3 and channel_id=3) then user2_id end),0) as rcmd_users_rcmded
                         , coalesce(count(distinct case when rc.impression_time is not null and (rcmd_type=3 and channel_id=3) then user2_id end),0) as rcmd_impression_users_rcmded
                        , coalesce(count(distinct case when (rc.like_time is not null or rc.dislike_time is not null) and (rcmd_type=3 and channel_id=3) then user2_id end),0) as rcmd_response_users_rcmded
                        , coalesce(count(distinct case when rc.friend_request_time is not null and (rcmd_type=3 and channel_id=3) then user2_id end),0) as rcmd_request_users_rcmded

                      from (select * from (select *
                                 , row_number() over (partition by user1_id, user2_id order by registered_time) as rn
                            from wippy_silver.hourly_merged_rcmd_log
                            where date_ymd_kst >= '2025-12-01'
                              and date_ymd_kst <= '2025-12-29'
                              and user1_gender = 1) where rn=1
                                    ) rc
                        join
                        (select id, gender, location, approval_time  from wippy_dump.accounts_user
                            where date(first_approval_time + interval '9' hour) between date('2025-12-01') and date('2025-12-28')
                           ) u on rc.user1_id = u.id
                            where rc.user1_relative_score<100
                            group by 1, 2, 3, 4, 5, 6) r on r.user_id=u.id and r.min_rc = u.minute_offset
                    where minute_offset*5 <= 60*24 and minute_offset >= 0
        )
