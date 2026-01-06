with rcmd_partition as
    (select user1_id, user2_id, registered_time,profile_open_time,impression_time,like_time,dislike_time,friend_request_time,other_accepted_time,joined_time
     from wippy_silver.hourly_merged_rcmd_log
    where date_ymd_kst between '2025-12-15' and '2026-01-04'
    ),
    user_info as (select user_id, gender
        , first_approval_time
        , first_approval_time + interval '9' hour as approval_time_kst
        , last_active_at + interval '9' hour as last_active_time_kst
        , date_diff('hour', first_approval_time, last_active_at) as retention_hour
    from wippy_silver.user_activation_metrics
    where first_approval_time >= cast ('2025-12-15 00:00:00' as timestamp) - interval '9' hour
    )

SELECT n.*
        , date(n.approval_time_kst) as approval_date
        , u.relative_score
        , u.location
        , vc.issue_cnt
        , check_coupon_box
        , coalesce(vc.redeem_cnt,0) as redeem_cnt
        , counts_1d_am,counts_2d_am,counts_4d_cl,counts_7d_am,counts_7d_cl
        , d1_retention, d2_4_retention, d5_7_retention, w1_retention
        , rcmd.*
        , rcmd_other_match+rcmd_match as rcmd_match_overall
        ,online_date
FROM user_info n
join (select id, gender, relative_score, location
      from wippy_dump.accounts_user
        where relative_score<100
      ) u on u.id = n.user_id
left join (SELECT user_id, count(issued_at_kst) as issue_cnt, count(redeemed_at_kst) as redeem_cnt
                FROM
                                                         (SELECT id,
         created_at + INTERVAL '9' HOUR AS issued_at_kst,
         received_at + INTERVAL '9' HOUR AS redeemed_at_kst,
         assigned_user_id as user_id,
         status,
         item_id
  FROM wippy_dump.mobile_coupon_box
  WHERE created_at >= cast (concat('{{start_date}}',' 00:00:00') as timestamp) - INTERVAL '9' HOUR
                                                         )
                                                         group by 1
            ) vc on vc.user_id = n.user_id
left join (select n.user_id
                    , count(distinct case when date_diff('day',date(n.approval_time_kst),ub.date_ymd_kst)=1 then 1 end) as d1_retention
                    , count(distinct case when date_diff('day',date(n.approval_time_kst),ub.date_ymd_kst) between 2 and 4 then 1 end) as d2_4_retention
                    , count(distinct case when date_diff('day',date(n.approval_time_kst),ub.date_ymd_kst) between 5 and 7 then 1 end) as d5_7_retention
                    , count(distinct case when date_diff('day',date(n.approval_time_kst),ub.date_ymd_kst) between 8 and 14 then 1 end) as w1_retention
                    , count(distinct case when date_diff('day',date(n.approval_time_kst),ub.date_ymd_kst) between 0 and 7 then ub.date_ymd_kst end) as online_date
                    , max(check_coupon_box) as check_coupon_box
            from (select user_id, date(date_ymd_kst) as date_ymd_kst
                    , max (case when (contains (navigations, 'mobile_coupon')) then 1 else 0 end) as check_coupon_box
                    from wippy_bronze.wippy_ubl
                    where date_ymd_kst between '2025-12-15' and '2026-01-04'
                    group by 1,2) ub
            join user_info n on ub.user_id = n.user_id
            group by 1) ub on ub.user_id = n.user_id
left join (select user1_id
                , r1.regi as rcmd_regi, imp as rcmd_imp, resp as rcmd_resp, r1.req as rcmd_req, r1.mat as rcmd_match, r2.regi as rcmd_other_regi,  r2.prof as rcmd_other_profile, r2.req as rcmd_other_request, r2.mat as rcmd_other_match,  r1.jo+r2.jo as rcmd_join,  r1.jo10+r2.jo10 as rcmd_join_10d,  r1.jo14+r2.jo14 as rcmd_join_14d
                from (select r1.user1_id
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.registered_time) <= 192 then r1.user2_id end) as regi
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.impression_time) <= 192 then r1.user2_id end) as imp
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.like_time) <= 192 or date_diff('hour',n.first_approval_time,r1.dislike_time) <= 192 then r1.user2_id end) as resp
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.friend_request_time) <= 192 then r1.user2_id end) as req
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.other_accepted_time) <= 192 then r1.user2_id end) as mat
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.joined_time) <= 192 then r1.user2_id end) as jo
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.joined_time) <= (24*10) then r1.user2_id end) as jo10
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.joined_time) <= (24*14) then r1.user2_id end) as jo14
                        from rcmd_partition r1
                        join user_info n on n.user_id = r1.user1_id
                        group by 1) r1
                left join (select r1.user2_id
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.registered_time) <= 192 then r1.user1_id end) as regi
--                         , count (distinct case when date_diff('hour',n.first_approval_time,r1.impression_time) <= 192 then r1.user2_id end) as imp
--                         , count (distinct case when date_diff('hour',n.first_approval_time,r1.like_time) <= 192 or date_diff('hour',n.first_approval_time,r1.dislike_time) <= 192 then r1.user2_id end) as resp
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.profile_open_time) <= 192 then r1.user1_id end) as prof
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.friend_request_time) <= 192 then r1.user1_id end) as req
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.other_accepted_time) <= 192 then r1.user1_id end) as mat
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.joined_time) <= 192 then r1.user1_id end) as jo
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.joined_time) <= (24*10) then r1.user1_id end) as jo10
                        , count (distinct case when date_diff('hour',n.first_approval_time,r1.joined_time) <= (24*14) then r1.user1_id end) as jo14
                        from rcmd_partition r1
                        join user_info n on n.user_id = r1.user2_id
                        group by 1) r2 on r1.user1_id = r2.user2_id
                )rcmd on rcmd.user1_id = n.user_id
left join (SELECT user_id
          , COUNT (distinct case when coupon_name like '%바나프레소 아메리카노%' then coupon_id end) AS counts_1d_am
          , COUNT (distinct case when coupon_name like '%아메리카노 1잔 2일차%' then coupon_id end) AS counts_2d_am
          , COUNT (distinct case when coupon_name like '%크리미라떼 1잔 4일차%' then coupon_id end) AS counts_4d_cl
          , COUNT (distinct case when coupon_name like '%크리미라떼 1잔 7일차%' then coupon_id end) AS counts_7d_cl
          , COUNT (distinct case when coupon_name like '%아메리카노 1잔 7일차%' then coupon_id end) AS counts_7d_am
            FROM ( SELECT id as coupon_id, created_at, source_id, used_at, assigned_user_id as user_id, status
                                    FROM wippy_dump.mobile_coupon_code
                                    WHERE status = 'USED'
                                    AND valid_from IS NOT NULL)
                LEFT JOIN ( SELECT id AS source_id,coupon_name, source_channel
                                    FROM wippy_dump.mobile_coupon_source ) USING (source_id)
            GROUP BY user_id) vu on vu.user_id = n.user_id
-- where vc.user_id is not null
                                                         order by 3
