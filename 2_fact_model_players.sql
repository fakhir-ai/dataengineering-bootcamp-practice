-- A query to deduplicate `game_details` from Day 1 so there's no duplicates

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
  
 create table fct_game_details(dim_game_date date,dim_season integer, dim_team_id integer, dim_player_id integer, dim_player_name text, dim_start_position text,
 dim_home_team_id boolean, dim_did_not_play boolean, dim_did_not_dress boolean, dim_not_with_team boolean, m_minutes_played real,
 m_fgm integer, m_fga integer, m_fg3m integer, m_fg3a integer, m_ftm integer, m_fta integer, m_oreb integer, m_dreb integer, m_reb integer, m_ast integer, 
 m_stl integer, m_blk integer, m_turnover integer, m_pf integer, m_pts integer, m_plus_minus integer, primary key (dim_game_date, dim_team_id, dim_player_id)
 );
 

create table user_cumulated(user_id text, dates_active date[], date date, primary key(user_id, date));

select min(event_time), max(event_time) from events;

insert into user_cumulated
WITH yesterday AS (select * from user_cumulated WHERE date = cast('2023-01-31' as date)) , 
today AS (SELECT cast(user_id as text) as user_id,CAST(event_time AS date) as date_active  FROM events WHERE 
CAST(event_time AS date) =cast('2023-02-01' as date) and user_id is not null group by user_id, CAST(event_time AS date))
SELECT coalesce(t.user_id,y.user_id) as user_id, 
case when y.dates_active is null then array[t.date_active]
     when t.date_active is null then y.dates_active
     else array[t.date_active] || y.dates_active  end as dates_active,
coalesce(t.date_active,y.date + interval ' 1 day') as date
FROM today t full outer join yesterday y on t.user_id = y.user_id; 

with users as (select * from user_cumulated where date='2023-01-31'),
     series as (select * from generate_series(cast('2023-01-01' as date),cast('2023-01-31' as date),interval '1 day') as series_date), 
     place_holder_ints as (select
		case when dates_active @> array[cast(series_date as date)] 
		then cast(power(2,32- (extract(day from age(date,series_date)))) as bigint) else 0 end as placeholder_int_value, *
		from users cross join series )
select user_id,cast(cast(sum(placeholder_int_value) as bigint) as bit(32)), 
BIT_COUNT(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_monthly_active, 
BIT_COUNT(cast('11111110000000000000000000000000' as bit(32)) & cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_weekly_active, 
BIT_COUNT(cast('10000000000000000000000000000000' as bit(32)) & cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_daily_active
from place_holder_ints group by 1;


create table array_metrics (user_id numeric, month_start date, metric_name text, metrics_array real[], primary key (user_id, month_start, metric_name));

insert into array_metrics
with daily_aggregate as (select user_id, cast(event_time as date) as date, count(1) as num_site_visits from events 
where user_id is not null and cast(event_time as date) = cast('2023-01-03' as date) group by user_id, cast(event_time as date)),
yesterday_array as (select * from array_metrics where month_start = cast('2023-01-01' as date))
select coalesce(da.user_id,ya.user_id) as user_id, 
coalesce(ya.month_start, date_trunc('month', da.date)) as month_start, 'site_hits' as metric_name, 
case when ya.metrics_array is not null then ya.metrics_array || array[coalesce(da.num_site_visits,0)] 
     when ya.metrics_array is null then  array_fill(0, array[coalesce(date  - cast(date_trunc('month', date) as date),0 )]) 
     || array[coalesce(da.num_site_visits,0)] end as metric_array
from daily_aggregate da full outer join yesterday_array ya on da.user_id = ya.user_id
on conflict(user_id, month_start, metric_name)
do update set metrics_array = excluded.metrics_array;

select cardinality(metrics_array), count(1) from array_metrics group by 1;

 with aggregate_sum as (select month_start, metric_name, array[SUM(metrics_array[1]), sum(metrics_array[2]), sum(metrics_array[3])] as sum_of_metrics
from array_metrics group by month_start, metric_name) 
select  metric_name,month_start, month_start+ CAST(cast(index - 1 as text) || 'day' as interval) as daily_date , element as value, index as index 
from aggregate_sum cross join  unnest(aggregate_sum.sum_of_metrics) with ordinality as t(element, index)

select *  from array_metrics;
