select *
     , num_profile_opened_users_today *100.0 / num_today_rcmd_users as rate_profile_today
      , num_chat_requested_users_today *100.0 / num_today_rcmd_users as rate_request_today
        , num_profile_opened_users_related *100.0 / num_related_rcmd_users as rate_profile_related
        , num_profile_opened_users_related *100.0 / num_profile_opened_users_today as rate_profile_related_per_today
from (select gender
           , exp_var
           , date_ymd_kst
           , user2_relative_score
           , count(user_id)                                                                             as num_users
           , count(case when channel_id = 21 then user_id end)                                          as num_today_rcmd_users
           , count(case when rcmd_type = 2060 then user_id end)                                         as num_related_rcmd_users
           , count(distinct case when channel_id = 21 then user_id end)                                          as num_today_rcmd_users_unique
           , count(distinct case when rcmd_type = 2060 then user_id end)                                         as num_related_rcmd_users_unique
           , count(case when channel_id = 21 and not rcmd_type = 2060 and profile_open_time is not null then user_id end)   as num_profile_opened_users_today
           , count(distinct case when channel_id = 21 and not rcmd_type = 2060 and profile_open_time is not null then user_id end)   as num_profile_opened_users_unique
           , count(case when channel_id = 21 and not rcmd_type = 2060 and friend_request_time is not null then user_id end) as num_chat_requested_users_today
           , count(case when rcmd_type = 2060 and profile_open_time is not null then user_id end)                           as num_profile_opened_users_related
            , count(case when rcmd_type = 2060 and friend_request_time is not null then user_id end)                         as num_chat_requested_users_related
      from (select r.user1_id as user_id,
                   u.exp_var,
                   r.user2_id,
                   r.user1_gender as gender,
                r.user2_relative_score,
                   r.channel_id,
                   r.rcmd_type,
                   r.date_ymd_kst,
                   r.profile_open_time,
                   r.friend_request_time
            from (select *
                  from wippy_silver.hourly_merged_rcmd_log
                  where date_ymd_kst > '2025-11-27'
                    and (rcmd_type = 2060 or channel_id = 21)
                    and impression_time is not null
                    ) r
            join (select *
                           from da_adhoc.exp_wp_4744
                           where exp_var is not null) u on u.user_id = r.user1_id
--                      left join (select quantity,
--                                        description,
--                                        item_id,
--                                        cast(source_info['recommend_id'] as bigint) as rcmd_id
--                                 from wippy_bronze.billings_jellyuselog
--                                 where date (date_ymd_kst) > date('2025-11-27')
--             and quantity<=0) j on j.rcmd_id = r.rcmd_id
           )
group by 1, 2, 3
order by 2, 1,3 )
order by 2, 1,3



