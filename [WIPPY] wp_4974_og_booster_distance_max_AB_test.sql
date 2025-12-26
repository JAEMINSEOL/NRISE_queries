select
        -- cast(rc.hour_kst as int)+cast(date_diff('day',date('2025-12-23'),date(rc.date_ymd_kst)) as int)*24 as hour_kst,
        mod(rc.user1_id,2) as user1_group
        -- , substr(cast(user1_location as varchar),1,3) as user1_location
        , rc.hour_kst
        -- , rc.rcmd_type
        -- , u1.location as user1_location
        , count(distinct rc.user1_id) as user_cnt
        , count(registered_time) as all_rcmd_cnt
        , count(distinct case when rc.exp_props['ml_ver'] is null then registered_time end) as rb_rcmd_cnt
        , count(case when rc.friend_request_time is not null or rr.friend_request_time is not null then registered_time end) as all_rcmd_friend_request_cnt
        , count(case when rc.other_accepted_time is not null or rr.other_accepted_time is not null then registered_time end) as all_rcmd_match_cnt
        , count(case when rc.like_time is not null then registered_time end) as all_rcmd_like_cnt
        , count(case when rc.rcmd_type = 80 then registered_time end) as og_booster_cnt
        , count(case when rc.rcmd_type = 80 and like_time is not null then like_time end) as og_booster_like_cnt
        , count(case when rc.rcmd_type = 80 and rr.friend_request_time is not null then registered_time end) as og_booster_friend_request_cnt
        , count(case when rc.rcmd_type = 80 and rr.other_accepted_time is not null then registered_time end) as og_booster_match_cnt
        , count(case when rc.rcmd_type = 80 and like_time is not null then registered_time end)*100.0 / count(case when rc.rcmd_type = 80 then registered_time end) as og_booster_like_ratio
        , count(case when rc.rcmd_type = 80 and rr.friend_request_time is not null then registered_time end)*100.0 / count(case when rc.rcmd_type = 80 then registered_time end) as og_booster_friend_request_ratio
        , count(case when rc.rcmd_type = 80 and rr.other_accepted_time is not null then registered_time end)*100.0 / count(case when rc.rcmd_type = 80 then registered_time end) as og_booster_match_ratio
        , avg(case when rc.rcmd_type=80 then min_distance end)/1000 as og_booster_distance
        , avg(case when rc.rcmd_type=80 and like_time is not null then min_distance end)/1000 as og_booster_like_distance
        , avg(case when rc.rcmd_type=80 and rr.friend_request_time is not null then min_distance end)/1000 as og_booster_friend_request_distance
from (select * 
        from wippy_silver.hourly_merged_rcmd_log 
        where ((date_ymd_kst = '2025-12-23' and cast(hour_kst as int)>11) or (date_ymd_kst in ('2025-12-24','2025-12-25')) or (date_ymd_kst = '2025-12-26' and cast(hour_kst as int)<12))
        and (rcmd_type in (4,50,80,101,102,103,105))
        ) rc
left join (select user1_id, user2_id, max(friend_request_time) as friend_request_time, max(other_accepted_time) as other_accepted_time, max(hour_kst) as hour_kst, max(date_ymd_kst) as date_ymd_kst, max(rcmd_type) as rcmd_type
        from wippy_silver.hourly_merged_rcmd_log
        where date_ymd_kst >= '2025-12-23'
        group by 1,2
        ) rr
        on rc.user1_id = rr.user1_id and rc.user2_id=rr.user2_id and rc.hour_kst=rr.hour_kst and rr.date_ymd_kst=rr.date_ymd_kst and rc.rcmd_type<>rr.rcmd_type
where (user1_gender=0 and user2_gender=1)

group by 1,2
having count(distinct rc.user1_id) > 5
order by 1 desc,2
