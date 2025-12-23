
select user2_gender as gender, relative_score_grade, min_j, avg(rcmd_users) as rcmd_users, avg(cumul_rcmd_users) as cumul_rcmd_users
from (select *
           , sum(rcmd_users) over (partition by user2_id order by min_j rows between unbounded preceding and current row) as cumul_rcmd_users
      from (select rc.user2_id,
                    floor(rc.user2_relative_score/10) +1 as relative_score_grade,
                   rc.user2_gender,
                   rc.date_ymd_kst,
                   date_diff('minute', cast(u.approval_time as timestamp), cast(rc.registered_time as timestamp)) as min_j
        , count(distinct user1_id) as rcmd_users
            from
                (select * from wippy_silver.hourly_merged_rcmd_log
                where date_ymd_kst >= '2025-12-01') rc
                join
                (select * from wippy_dump.accounts_user
                where date_ymd_snapshot >= '2025-12-01') u
            on rc.user2_id = u.id and date (rc.date_ymd_kst) <= date_add('day',1, date (u.date_ymd_snapshot))
            
            group by 1, 2, 3, 4, 5)
            where min_j <= 60*24 and min_j >= 0
            )
            where user2_gender=1
            and relative_score_grade <=10
            group by 1,2,3
order by 1,2
