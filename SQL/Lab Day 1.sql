select * from player_seasons ps;

-- Create Types
create type season_stats as ( 
	season INTEGER,
	gp INTEGER,
	pts real,
	reb real,
	ast real
)

create type scoring_class as enum('star', 'good', 'average', 'bad')

-- Create cumulative table
create table players (
	player_name text,
	height text,
	college text,
	country text,
	draft_year text,
	draft_round text,
	draft_number text,
	season_stats season_stats[],
	scoring_class scoring_class,
	years_since_last_season INTEGER,
	current_season INTEGER,
	primary key (player_name, current_season)
)

-- Insert data into cumulative table, many rows for same player (many seasons) each with data until that season

insert into players 
with yesterday as (
	select * from players
	where current_season = 2001
),
today as (
	select * from player_seasons ps 
	where season = 2002
)

select 
	coalesce(t.player_name, y.player_name) as player_name
	, coalesce(t.height, y.height) as height
	, coalesce(t.college, y.college) as college
	, coalesce(t.country, y.country) as country
	, coalesce(t.draft_year, y.draft_year) as draft_year
	, coalesce(t.draft_round, y.draft_round) as draft_round
	, coalesce(t.draft_number, y.draft_number) as draft_number
	, case when y.season_stats is null
		then array[row(
			t.season
			, t.gp
			, t.pts
			, t.reb
			, t.ast
		)::season_stats]
	when t.season is not null then y.season_stats || array[row(
			t.season
			, t.gp
			, t.pts
			, t.reb
			, t.ast
		)::season_stats]
	else y.season_stats
	end as season_stats
	, case when t.season is not null then 
		case when t.pts > 20 then 'star'
			when t.pts > 15 then 'good'
			when t.pts > 10 then 'average'
			else 'bad'
		end::scoring_class
	else y.scoring_class
	end as scoring_class
	, case when t.season is not null then 0
		else y.years_since_last_season + 1
	end as years_since_last_season 	
	, coalesce (t.season, y.current_season + 1) as current_season
	
from today t full outer join yesterday y
on t.player_name = y.player_name;

-- Explode a specific season to recover original data

with unnested as (
	select player_name, unnest(season_stats)::season_stats as season_stats from players
	where current_season = 1996
	and player_name = 'Michael Jordan'
	)
select player_name, (season_stats::season_stats).* from unnested

-- Procedure to load all years (Not par)t of the course)
DO $$
DECLARE
    year INTEGER;
    sql_query TEXT;
BEGIN
    FOR year IN 1995..2021 LOOP
        -- Construct the SQL as a text string
        sql_query := 
            'INSERT INTO players ' ||
            'WITH yesterday AS (' ||
            '    SELECT * FROM players WHERE current_season = ' || year || '),' ||
            'today AS (' ||
            '    SELECT * FROM player_seasons ps WHERE season = ' || (year + 1) || ')' ||
            'SELECT ' ||
            '    COALESCE(t.player_name, y.player_name) AS player_name, ' ||
            '    COALESCE(t.height, y.height) AS height, ' ||
            '    COALESCE(t.college, y.college) AS college, ' ||
            '    COALESCE(t.country, y.country) AS country, ' ||
            '    COALESCE(t.draft_year, y.draft_year) AS draft_year, ' ||
            '    COALESCE(t.draft_round, y.draft_round) AS draft_round, ' ||
            '    COALESCE(t.draft_number, y.draft_number) AS draft_number, ' ||
            '    CASE WHEN y.season_stats IS NULL THEN ' ||
            '        ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats] ' ||
            '        WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats] ' ||
            '        ELSE y.season_stats ' ||
            '    END AS season_stats, ' ||
            '    CASE WHEN t.season IS NOT NULL THEN ' ||
            '        CASE WHEN t.pts > 20 THEN ''star'' ' ||
            '            WHEN t.pts > 15 THEN ''good'' ' ||
            '            WHEN t.pts > 10 THEN ''average'' ' ||
            '            ELSE ''bad'' ' ||
            '        END::scoring_class ' ||
            '        ELSE y.scoring_class ' ||
            '    END AS scoring_class, ' ||
            '    CASE WHEN t.season IS NOT NULL THEN 0 ' ||
            '        ELSE y.years_since_last_season + 1 ' ||
            '    END AS years_since_last_season, ' ||
            '    COALESCE(t.season, y.current_season + 1) AS current_season ' ||
            'FROM today t FULL OUTER JOIN yesterday y ' ||
            'ON t.player_name = y.player_name;';

        -- Log the generated query for debugging
        RAISE NOTICE 'Executing SQL: %', sql_query;

        -- Execute the generated query
        EXECUTE sql_query;
    END LOOP;
END $$;

-- Analytical queries (Notice not use of group by) This query is paralellizable and very fast

select 
	player_name, 
	season_stats[cardinality(season_stats)].pts /
	case when season_stats[1].pts = 0 then 1 else season_stats[1].pts end as improvement,
	season_stats[1] as first_season,
	season_stats[cardinality(season_stats)] as latest_season
from players
where current_season = 2001
and scoring_class = 'star'
order by improvement desc -- only slow part
