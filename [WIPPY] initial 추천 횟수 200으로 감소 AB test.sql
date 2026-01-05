with rcmd_part as
    (select user1_id, user1_gender, user1_relative_score, user1_location, user2_id, user2_gender, user2_relative_score, user2_location
            , registered_time, impression_time, like_time, dislike_time, friend_request_time, other_accepted_time, joined_time
            , case when rcmd_type=50 then 50000 else rcmd_type end as rcmd_type
            , channel_id
    from wippy_silver.hourly_merged_rcmd_log 
    where date_ymd_kst between '2025-12-30' and '2026-01-04'
    )

select mod(r.user_id,2) as user_group, r.gender, approval_date
        , count(distinct r.user_id) as user_cnts
        , count(distinct case when deleted_date is not null then r.user_id end) as deleted_users 
        , count(distinct case when date_diff('hour',first_approval_time,ci_hash_deleted_time ) <= 24 then r.user_id end) as h24_deleted_users 
        , avg(initial_rcmd_cnts) as initial_rcmd_cnts
        , avg(initial_imp_cnts) as initial_imp_cnts
        , avg(initial_resp_cnts) as initial_resp_cnts
        , avg(initial_match_cnts) as initial_match_cnts
        , avg(other_rcmd_cnts) as other_rcmd_cnts
        , avg(other_match_cnts) as other_match_cnts
        , avg(overall_match_cnts) as overall_match_cnts
        
from(
    select user2_id as user_id, user2_gender as gender
            , count(distinct case when rcmd_type = 50000 then user1_id end) as initial_rcmd_cnts
            , count(distinct case when rcmd_type = 50000 and impression_time is not null then user1_id end) as initial_imp_cnts
            , count(distinct case when rcmd_type = 50000 and impression_time is not null and coalesce(like_time,dislike_time) is not null then user1_id end) as initial_resp_cnts
            , count(distinct case when rcmd_type = 50000 and coalesce(other_accepted_time, r2_other_accepted_time) is not null then user1_id end) as initial_match_cnts
            , count(distinct case when rcmd_type <> 50000 then user1_id end) as other_rcmd_cnts
            , count(distinct case when rcmd_type <> 50000 and coalesce(other_accepted_time, r2_other_accepted_time) is not null then user1_id end) as other_match_cnts
            , count(distinct case when (other_accepted_time is not null or r2_other_accepted_time is not null) then user1_id end) as overall_match_cnts
    from 
    (select *, row_number() over (partition by user1_id, user2_id order by rcmd_type desc) as rn
    from rcmd_part r1
    join (select user1_id as user1_rcmd_id, user2_id as user2_rcmd_id, other_accepted_time as r2_other_accepted_time from rcmd_part) r2 on r2.user1_rcmd_id = r1.user1_id and r2.user2_rcmd_id = r1.user2_id
    )
    where rn=1
    group by 1,2
) r
join (
    select user_id, date(first_approval_time+interval '9' hour) as approval_date, date(ci_hash_deleted_time + interval '9' hour) as deleted_date,first_approval_time,ci_hash_deleted_time
    from wippy_silver.user_activation_metrics
    where date(first_approval_time+interval '9' hour) between date('2025-12-30') and date('2026-01-03')
    ) u on u.user_id = r.user_id
where r.gender=1
group by 1,2,3
order by 1,3
