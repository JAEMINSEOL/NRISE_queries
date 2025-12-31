SELECT n.*
        , date(n.approval_time_kst) as approval_date
        , u.relative_score
        , u.location
        , case when vc.user_id is not null then 1 else 0 end as get_vana_coupon
        , vu.counts as used_vana_coupons
FROM
    (select user_id, gender
        , first_approval_time + interval '9' hour as approval_time_kst
        , last_active_at + interval '9' hour as last_active_time_kst
        , date_diff('hour', first_approval_time, last_active_at) as retention_hour
    from wippy_silver.user_activation_metrics
    where first_approval_time >= cast ('2025-12-20 00:00:00' as timestamp) - interval '9' hour
    ) n
join (select id, gender, relative_score, location
      from wippy_dump.accounts_user
        where relative_score<100
      ) u on u.id = n.user_id
left join (SELECT distinct user_id FROM
                                                         (SELECT id,
         created_at + INTERVAL '9' HOUR AS issued_at_kst,
         received_at + INTERVAL '9' HOUR AS redeemed_at_kst,
         assigned_user_id as user_id,
         status,
         item_id
  FROM wippy_dump.mobile_coupon_box
  WHERE created_at >= TIMESTAMP '2025-12-22 00:00:00' - INTERVAL '9' HOUR
                                                         and received_at is not null)
--                                                          wippy_bronze.wippy_ubl
--             where date_ymd_kst >= '2025-12-20'
-- --             and contains(navigations,'home')
--             and (event_props['banner_title'] like '%커피 4잔을%' or contains(navigations, 'onboarding_step_mobile_coupon')
--                                                          or (contains(navigations, 'mobile_coupon') and event_type = 'click'))
            ) vc on vc.user_id = n.user_id
left join (SELECT user_id, COUNT (distinct coupon_id) AS counts
            FROM ( SELECT id as coupon_id, created_at, source_id, used_at, assigned_user_id as user_id
                                    FROM wippy_dump.mobile_coupon_code
                                    WHERE status = 'USED'
                                    AND valid_from IS NOT NULL)
                LEFT JOIN ( SELECT id AS source_id,coupon_name, source_channel
                                    FROM wippy_dump.mobile_coupon_source ) USING (source_id)
            GROUP BY user_id) vu on vu.user_id = n.user_id
-- where vc.user_id is not null
                                                         order by 3
