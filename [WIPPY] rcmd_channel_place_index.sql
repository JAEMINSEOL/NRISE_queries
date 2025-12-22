SELECT distinct g.channel_id, s.*, c.name as channel_name
, p.title as place_title

FROM wippy_gold.daily_rcmd_agg g
left join wippy_silver.rcmd_type_mapping2 s on s.rcmd_type = g.rcmd_type
left join (select id, name from wippy_dump.channels_channel_partitioned where date_ymd_snapshot>'2025-12-10') c on c.id = g.channel_id
left join (select id, title from wippy_dump."entertainments_entertainment_partitioned where date_ymd_snapshot > '2025-12-10') p on cast(p.id as varchar) = substring(c.name, 17,3)
where date_ymd_kst > '2025-12-10'
order by 2,1
