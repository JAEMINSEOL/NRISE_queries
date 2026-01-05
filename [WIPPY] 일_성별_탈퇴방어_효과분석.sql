WITH navigation_events AS (
    SELECT
        CAST(date_ymd_kst AS DATE) AS event_date,
        user_id
    FROM wippy_bronze.wippy_ubl
    WHERE date_ymd_kst BETWEEN '2025-12-20' AND '2026-01-04'
      AND cardinality(filter(navigations, x -> x = 'user_delete_remaining_reward')) > 0
      AND user_id IS NOT NULL
),
continue_events AS (
    SELECT
        CAST(date_ymd_kst AS DATE) AS event_date,
        user_id
    FROM wippy_bronze.wippy_ubl
    WHERE date_ymd_kst BETWEEN '2025-12-20' AND '2026-01-04'
      and event_props['object_value'] = 'continue' 
      AND user_id IS NOT NULL
), 
user_gender AS(
SELECT id as user_id, gender
FROM wippy_dump.accounts_user
)
SELECT
    nav.event_date,
    u.gender AS gender,
    COUNT(DISTINCT nav.user_id) AS unique_user_delete_remaining_reward_count,
    COUNT(DISTINCT cont.user_id) AS unique_continue_user_count
FROM (
    SELECT
        event_date,
        user_id
    FROM navigation_events
) nav
LEFT JOIN (
    SELECT
        event_date,user_id
    FROM continue_events
) cont
ON nav.event_date = cont.event_date AND nav.user_id = cont.user_id
LEFT JOIN user_gender u ON u.user_id = nav.user_id
GROUP BY nav.event_date, u.gender
ORDER BY nav.event_date, u.gender
