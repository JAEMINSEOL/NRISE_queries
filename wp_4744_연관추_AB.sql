create table da_adhoc.exp_wp_4744 as
select distinct user_id, exp_props['WP-4744'] as exp_var
from wippy_bronze.wippy_ubl
where date(date_ymd_kst)  > date('2025-11-27')

select u.exp_var, r.*
from (select *
      from wippy_silver.hourly_merged_rcmd_log
      where date_ymd_kst = '2025-12-15'
        and rcmd_type = 2060) r
join (select * from da_adhoc.exp_wp_4744 where exp_var is not null
        ) u on u.user_id = r.user1_id
