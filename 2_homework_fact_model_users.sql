-- A query to deduplicate `game_details` from Day 1 so there's no duplicates

 create table fct_game_details(dim_game_date date,dim_season integer, dim_team_id integer, dim_player_id integer, dim_player_name text, dim_start_position text,
 dim_home_team_id boolean, dim_did_not_play boolean, dim_did_not_dress boolean, dim_not_with_team boolean, m_minutes_played real,
 m_fgm integer, m_fga integer, m_fg3m integer, m_fg3a integer, m_ftm integer, m_fta integer, m_oreb integer, m_dreb integer, m_reb integer, m_ast integer, 
 m_stl integer, m_blk integer, m_turnover integer, m_pf integer, m_pts integer, m_plus_minus integer, primary key (dim_game_date, dim_team_id, dim_player_id)
 );

insert into fct_game_details
with game_deduped as (select g.game_date_est, g.season, g.home_team_id, gd.*,
row_number() over(partition by gd.game_id, team_id, player_id order by g.game_date_est) as rownum
from game_details gd join games g on gd.game_id = g.game_id) 
select game_date_est, season, team_id, player_id, player_name, start_position, team_id = home_team_id as dim_home_team_id,
coalesce(position('DNP' in comment),0) > 0 as dim_did_not_play, coalesce(position('DND' in comment),0) > 0 as dim_did_not_dress,
coalesce(position('NWT' in comment),0) > 0 as dim_not_with_team,
cast(split_part(min,':',1) as real) +  cast(split_part(min,':',2) as real)/60 as minutes__played,
fgm, fga,  fg3m, fg3a, ftm, fta, oreb, dreb, reb, ast, stl, blk, "TO" as turn_over, pf, pts, plus_minus
  from game_deduped where rownum=1;
  

/* - A DDL for an `user_devices_cumulated` table that has:
  - a `device_activity_datelist` which tracks a users active days by `browser_type`
  - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
    - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!) */

CREATE TABLE user_devices_cumulated (
	device_id numeric NOT NULL,
	user_id numeric NOT NULL,
	browser_type text NULL,
	device_activity_datelists date[],
	"date" date NOT NULL,
	CONSTRAINT user_devices_cumulated_pkey PRIMARY KEY (user_id, device_id, date)
);

-- A cumulative query to generate `device_activity_datelist` from `events`
insert into user_devices_cumulated 
with dedup_device_events as (select user_id, event_time, e.device_id as device_id, browser_type,
row_number() over(partition by user_id,event_time, e.device_id order by event_time) as rownum
from devices d join events e  on d.device_id = e.device_id where user_id is not null and CAST(event_time AS date) =cast('2023-02-01' as date) ), 
 today as (select user_id,cast(event_time as date) as date_active,device_id,browser_type  from dedup_device_events where rownum = 1 
 group by user_id,cast(event_time as date),device_id,browser_type ), 
 yesterday as (select * from user_devices_cumulated where date = cast('2023-01-31' as date))
 select coalesce(t.device_id, y.device_id) as device_id, coalesce(t.user_id, y.user_id) as user_id, 
 coalesce (t.browser_type, y.browser_type) as browser_type,
 case when y.device_activity_datelists is null then array[t.date_active]
      when t.date_active is null then y.device_activity_datelists
      else  array[t.date_active] || y.device_activity_datelists end as device_activity_datelist, 
 coalesce(t.date_active, y.date + interval '1 day') as date
 from today t full outer join yesterday y on t.user_id = y.user_id and t.device_id = y.device_id;


-- A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 

with users as (select * from user_devices_cumulated where date='2023-01-31'),
     series as (select * from generate_series(cast('2023-01-01' as date),cast('2023-01-31' as date),interval '1 day') as series_date), 
     place_holder_ints as (select
		case when device_activity_datelists @> array[cast(series_date as date)] 
		then cast(power(2,32- (extract(day from age(date,series_date)))) as bigint) else 0 end as placeholder_int_value, *
		from users cross join series )
select user_id,device_id,browser_type,cast(cast(sum(placeholder_int_value) as bigint) as bit(32)) as datelist, 
BIT_COUNT(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_monthly_active, 
BIT_COUNT(cast('11111110000000000000000000000000' as bit(32)) & cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_weekly_active, 
BIT_COUNT(cast('10000000000000000000000000000000' as bit(32)) & cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_daily_active
from place_holder_ints group by 1,2,3;

/* - A DDL for `hosts_cumulated` table 
  - a `host_activity_datelist` which logs to see which dates each host is experiencing any activity */

CREATE TABLE hosts_cumulated (
	user_id numeric,
	host text, 
	url text, 
	host_count integer[], 
	host_activity_datelist date[],
	"date" date,
	PRIMARY KEY (user_id,host, url, date)
);

-- The incremental query to generate `host_activity_datelist`
insert into hosts_cumulated
with dedup_host_events as (select user_id,host, event_time, url,
row_number() over(partition by user_id,host,event_time, url order by event_time) as rownum
from events where CAST(event_time AS date) =cast('2023-01-03' as date) and user_id is not null -- and host='www.eczachly.com' and user_id = 16217504073105100000
), 
 today as (select user_id,host,url,cast(event_time as date) as date_active, count(event_time) as host_count 
 from dedup_host_events where rownum = 1 
 group by user_id,host,url,cast(event_time as date)), 
 yesterday as (select * from hosts_cumulated where date = cast('2023-01-02' as date))
 select coalesce(t.user_id, y.user_id) as user_id,coalesce(t.host, y.host) as host, coalesce(t.url, y.url) as url, 
case when y.host_count is null then array[t.host_count] 
     when t.host_count is null then y.host_count 
     else array[t.host_count] || y.host_count end as host_count,
 case when y.host_activity_datelist is null then array[t.date_active]
      when t.date_active is null then y.host_activity_datelist
      else  array[t.date_active] || y.host_activity_datelist end as host_activity_datelist, 
 coalesce(t.date_active, y.date + interval '1 day') as date
 from today t full outer join yesterday y on t.user_id = y.user_id and t.host = y.host and t.url = y.url;

/* - A monthly, reduced fact table DDL `host_activity_reduced`
   - month
   - host
   - hit_array - think COUNT(1)
  - unique_visitors array -  think COUNT(DISTINCT user_id) */

create table host_activity_reduced (user_id numeric, host text, url text, month_start date, metric_name text, hit_array integer[], 
unique_visitors_array integer[],
primary key (user_id, host,url,month_start, metric_name));

/* - An incremental query that loads `host_activity_reduced`
  - day-by-day */


insert into host_activity_reduced
with daily_aggregate as (select user_id, host, url, cast(event_time as date) as date, count(1) as num_site_visits,
count(distinct user_id) as unique_visits from events 
where user_id is not null and cast(event_time as date) = cast('2023-01-08' as date) 
group by user_id,host, url, cast(event_time as date)),
yesterday_array as (select * from host_activity_reduced where month_start = cast('2023-01-07' as date))
select coalesce(da.user_id,ya.user_id) as user_id, coalesce(da.host,ya.host) as host, coalesce(da.url,ya.url) as url, 
coalesce(ya.month_start, date_trunc('month', da.date)) as month_start, 'site_hits' as metric_name, 
case when ya.hit_array is not null then ya.hit_array || array[coalesce(da.num_site_visits,0)] 
     when ya.hit_array is null then  array_fill(0, array[coalesce(date  - cast(date_trunc('month', date) as date),0 )]) 
     || array[coalesce(da.num_site_visits,0)] end as hit_array,
case when ya.unique_visitors_array is not null then ya.unique_visitors_array || array[coalesce(da.unique_visits,0)] 
     when ya.unique_visitors_array is null then  array_fill(0, array[coalesce(date  - cast(date_trunc('month', date) as date),0 )]) 
     || array[coalesce(da.unique_visits,0)] end as unique_visitors_array
from daily_aggregate da full outer join yesterday_array ya on da.user_id = ya.user_id and da.host = ya.host and da.url = ya.url
on conflict(user_id, host, url, month_start, metric_name)
do update set hit_array = excluded.hit_array, unique_visitors_array = excluded.unique_visitors_array;
