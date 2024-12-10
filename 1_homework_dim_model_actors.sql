select count(*) from actor_films af;

/* 1. **DDL for `actors` table:** Create a DDL for an `actors` table with the following fields:
    - `films`: An array of `struct` with the following fields:
		- film: The name of the film.
		- votes: The number of votes the film received.
		- rating: The rating of the film.
		- filmid: A unique identifier for each film.

    - `quality_class`: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:
		- `star`: Average rating > 8.
		- `good`: Average rating > 7 and ≤ 8.
		- `average`: Average rating > 6 and ≤ 7.
		- `bad`: Average rating ≤ 6.
    - `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year). */
    
create type quality_class as enum('star','good','average','bad');
create type films as (film text, votes integer, rating real, filmid text);
create table actors (actor text,current_year integer, films films[],quality_class quality_class,is_active bool,primary key(actor,current_year,films));

-- 2. **Cumulative table generation query:** Write a query that populates the `actors` table one year at a time.

insert into actors
with yesterday as (select * from actors af where current_year=1975), 
     today as (select * from actor_films af where year=1976)
select
coalesce(y.actor,t.actor) as actor, 
coalesce(t.year,y.current_year+1) as current_year,
case when y.films is null then array[row(t.film,t.votes,t.rating,t.filmid)::films] 
     when t.year is not null then y.films || array[row(t.film,t.votes,t.rating,t.filmid)::films] 
     else y.films  end as films, 
 case when t.year is not null then case when rating > 8 then 'star'
 									    when rating > 7 and rating <= 8 then 'good'
 									    when rating > 6 and rating <= 7 then 'average'
 									    else 'bad' end :: quality_class
 							  else y.quality_class end as quality_class, 
 case when t.year is null then false else true end as is_boolean
from today t full outer join yesterday y on t.actor = y.actor ;

select * from actor_films af where actor='Abe Vigoda'
SELECT * FROM actors WHERE actor='Abe Vigoda'

/* 3. **DDL for `actors_history_scd` table:** Create a DDL for an `actors_history_scd` table with the following features:
    - Implements type 2 dimension modeling (i.e., includes `start_date` and `end_date` fields).
    - Tracks `quality_class` and `is_active` status for each actor in the `actors` table. */

create table actors_scd (actor text, quality_class text, is_active boolean, start_date integer, end_date integer, current_year integer, primary key(actor,start_date,quality_class));

-- 4. **Backfill query for `actors_history_scd`:** Write a "backfill" query that can populate the entire `actors_history_scd` table in a single query.
insert into actors_scd
with with_previous as 
(
select actor, current_year,
quality_class, LAG(quality_class,1) over (partition by actor order by current_year) as previous_quality_class, 
is_active, LAG(is_active,1) over (partition by actor order by current_year) as previous_is_active
from actors)
, with_change_indicator as
( select *, case when quality_class <> previous_quality_class then 1
                 when is_active <> previous_is_active then 1 
                 else  0 end as change_indicator
 from with_previous) 
 , with_streak_indicator as 
 (select actor, quality_class,is_active, current_year,sum(change_indicator) over (partition by actor order by current_year) as streak_indicator from with_change_indicator)
 select actor, quality_class, is_active, min(current_year) as start_date, max(current_year) as end_date, max(current_year) as current_year
 from with_streak_indicator 
 group by actor, quality_class, is_active, streak_indicator
 order by actor, current_year
 
 
 -- 5. **Incremental query for `actors_history_scd`:** Write an "incremental" query that combines the previous year's SCD data with new incoming data from the `actors` table.

 create type actor_scd_type as (quality_class quality_class, is_active boolean, start_date integer, end_date integer);


 with
 last_season_scd as (select actor, quality_class, is_active, start_date, end_date from actors_scd where current_year= 1975 and end_date=1975), 
 historical_scd as (select actor, quality_class::quality_class, is_active, start_date, end_date from actors_scd where current_year= 1975 and end_date<1975), 
 current_season_scd as (select distinct actor, quality_class, is_active, current_year as start_date, current_year as end_date from actors where current_year=1976),
 unchanged_scd as (select curr.actor, curr.quality_class, curr.is_active, last.start_date, curr.end_date
 from current_season_scd curr join last_season_scd last on curr.actor = last.actor
 where  curr.is_active = last.is_active and curr.quality_class::quality_class = last.quality_class::quality_class), 
 changed_scd as (select curr.actor, UNNEST(array[row(curr.quality_class::quality_class, curr.is_active, curr.start_date, curr.end_date)::actor_scd_type,
 row(last.quality_class::quality_class, last.is_active, last.start_date,last.end_date)::actor_scd_type]) as records 
 from current_season_scd curr left join last_season_scd last on curr.actor=last.actor 
 where curr.quality_class::quality_class <> last.quality_class::quality_class and curr.is_active <> last.is_active),
 nested_changed_scd as (select actor,  (records::actor_scd_type).quality_class::quality_class,(records::actor_scd_type).is_active, (records::actor_scd_type).start_date,
 (records::actor_scd_type).end_date from changed_scd),
 new_record_scd as (select curr.actor, curr.quality_class::quality_class, curr.is_active, curr.start_date, curr.end_date 
 from current_season_scd curr left join last_season_scd last on curr.actor = last.actor where last.actor is null)
 select * from historical_scd 
 union all
 select * from unchanged_scd
 union all 
 select * from nested_changed_scd 
 union all 
 select * from new_record_scd;
 
 