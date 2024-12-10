create type season_stats as (season integer, gp integer, pts real, reb real, ast real );

create type scoring_class as enum('star','good','average','bad');

create table players (
player_name text, 
college text, 
country text, 
draft_year text, 
season_stats season_stats[],
scoring_class scoring_class, 
years_since_last_season integer,
current_season integer,
is_active boolean,
primary key(player_name, current_season)
);


insert into players
with yesterday as (select * from players where current_season = 2021), 
     today as (select * from player_seasons where season = 2022)
select
coalesce(y.player_name,t.player_name) as player_name, 
coalesce(y.college,t.college) as college, 
coalesce(y.country,t.country) as country, 
coalesce(y.draft_year,t.draft_year) as draft_year,
case when y.season_stats is null then array[row(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
     when t.season is not null then y.season_stats || array[row(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
     else y.season_stats end as season_stats,
case when t.season is not null then
								case when pts>20 then 'star' 
								     when pts>15 then 'good'
								     when pts>10 then 'average'
								     else 'bad' end :: scoring_class
	 else y.scoring_class end as scoring_class,
case when t.season is not null then 0
	 else y.years_since_last_season + 1 end as years_since_last_season,
coalesce(t.season,y.current_season+1) as current_season, 
case when t.season is null then false else true end as is_active
from today t full outer join yesterday y on y.player_name = t.player_name; 

select player_name, unnest(season_stats), scoring_class,years_since_last_season, current_season
from players where  player_name = 'Michael Jordan' and current_season =2000;

select player_name, (season_stats[cardinality(season_stats)]::season_stats).pts / 
case when (season_stats[1]::season_stats).pts = 0 then 1 else (season_stats[1]::season_stats).pts end as improved_player
from players where current_season =2001 and scoring_class ='star'
order by improved_player desc;

create table players_scd(player_name text, scoring_class text, start_season integer, end_season integer, current_season integer, is_active boolean, primary key(player_name, start_season));


insert into players_scd
with with_previous as (
select player_name, current_season,
scoring_class, LAG(scoring_class,1) over (partition by player_name order by current_season) as previous_scoring_class, 
is_active , LAG(is_active,1) over (partition by player_name order by current_season) as previous_is_active
from players
) 
, with_indicators as (
select *, case when scoring_class <> previous_scoring_class then 1 
				when is_active <> previous_is_active then 1
				else 0 end as scoring_class_change_indicator
from with_previous) 
, with_streaks as (
select * , SUM(scoring_class_change_indicator) over (partition by player_name order by current_season) as streak_indicator 
from with_indicators)
select player_name, scoring_class, min(current_season) as start_season, max(current_season) as end_season, 2021 as current_season, is_active
from with_streaks group by player_name,scoring_class,is_active, streak_indicator
order by player_name, start_season;


select max(current_season) from players where player_name = 'Aaron McKie'


create type scd_type as (scoring_class scoring_class,is_active boolean, start_season integer, end_season integer );

with last_season_scd as (select * from players_scd where current_season=2021 and end_season = 2021 ), 
     historical_scd as (select player_name, scoring_class::scoring_class, is_active, start_season, end_season from players_scd  where current_season=2021 and end_season < 2021), 
     current_season_scd as (select * from players where current_season=2022), 
     unchanged_scd as (select curr.player_name, curr.scoring_class, curr.is_active, 
     				   last.start_season as start_season, curr.current_season as end_season
						from current_season_scd curr join last_season_scd last on curr.player_name = last.player_name 
						where curr.is_active = last.is_active), 
						--  and curr.scoring_class = last.scoring_class)
     changed_scd as (select curr.player_name 
     				   , unnest(ARRAY[row( last.scoring_class, last.is_active, last.start_season, last.end_season)::scd_type, 
     				   row(curr.scoring_class, curr.is_active, curr.current_season, curr.current_season)::scd_type]) as records
						from current_season_scd curr left join last_season_scd last on curr.player_name = last.player_name 
						where curr.is_active <> last.is_active),
	 unnested_changed_scd as (select player_name, (records::scd_type).scoring_class, (records::scd_type).is_active,(records::scd_type).start_season, (records::scd_type).end_season from changed_scd), 
	 new_changed_scd as (select curr.player_name, curr.scoring_class, curr.is_active, curr.current_season as start_season, curr.current_season as end_season
	 from current_season_scd curr left join last_season_scd last on curr.player_name = last.player_name where curr.player_name is null )
	 select * from new_changed_scd
	 union all 
	 select * from unchanged_scd
	 union all 
	 select * from unnested_changed_scd
	 union all
	 select * from historical_scd;
	

	




















