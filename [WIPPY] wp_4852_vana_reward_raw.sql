SELECT n.*
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
    where first_approval_time >= cast ('2025-12-17 00:00:00' as timestamp) - interval '9' hour
    ) n
join (select id, gender, relative_score, location
      from wippy_dump.accounts_user
      ) u on u.id = n.user_id
left join (SELECT distinct user_id FROM wippy_bronze.wippy_ubl
            where date_ymd_kst >= '2025-12-17'
            and contains(navigations, 'onboarding_step_mobile_coupon')
            ) vc on vc.user_id = n.user_id
left join (SELECT user_id, COUNT (distinct coupon_id) AS counts
            FROM ( SELECT id as coupon_id, created_at, source_id, used_at, assigned_user_id as user_id
                                    FROM wippy_dump.mobile_coupon_code
                                    WHERE status = 'USED'
                                    AND valid_from IS NOT NULL)
                LEFT JOIN ( SELECT id AS source_id,coupon_name, source_channel
                                    FROM wippy_dump.mobile_coupon_source ) USING (source_id)
            GROUP BY user_id) vu on vu.user_id = n.user_id