create table da_adhoc.exp_wp_4744 as
select distinct user_id, exp_props['WP-4744'] as exp_var
from wippy_bronze.wippy_ubl
where date(date_ymd_kst)  > date('2025-11-27')

select r.rcmd_id, r.user_id, u.exp_var, r.rcmd_user_id, r.rcmd_context, r.date_ymd_kst, j.quantity
from (select *
      from wippy_bronze.hourly_rcmd_log
      where date_ymd_kst > '2025-11-27'
        and rcmd_type = 2060) r
join (select * from da_adhoc.exp_wp_4744 where exp_var is not null
        ) u on u.user_id = r.user_id
left join (select quantity, description, item_id, cast(source_info['recommend_id'] as bigint) as rcmd_id
          from wippy_bronze.billings_jellyuselog
          where date(date_ymd_kst) > date('2025-11-27')
            and quantity<0) j on j.rcmd_id = r.rcmd_id
order by quantity




