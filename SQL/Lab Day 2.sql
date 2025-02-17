SELECT * FROM player_seasons ps;

-- Create Types
CREATE TYPE season_stats AS ( 
	season INTEGER,
	gp INTEGER,
	pts real,
	reb real,
	ast real
);

CREATE TYPE scoring_class AS enum('star', 'good', 'average', 'bad');


-- Create cumulative table
CREATE TABLE players (
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
	is_active BOOLEAN,
	primary key (player_name, current_season)
);

-- Create SCD table
CREATE TABLE players_scd_table
(
	player_name text,
	scoring_class scoring_class,
	is_active boolean,
	start_season integer,
	end_season integer
	current_season INTEGER
);

-- Insert data into cumulative table, many rows for same player (many seasons) each with data until that season

INSERT INTO players
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
), p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.gp,
                            ps.pts,
                            ps.reb,
                            ps.ast
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
    w.season,
    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;


-- Insert SCD for scoring class
INSERT INTO players_scd_table
WITH streak_started AS (
SELECT player_name,
       current_season,
       scoring_class,
       is_active,
       LAG(scoring_class, 1) OVER
           (PARTITION BY player_name ORDER BY current_season) <> scoring_class
           OR LAG(scoring_class, 1) OVER
           (PARTITION BY player_name ORDER BY current_season) IS NULL
           OR LAG(is_active, 1) OVER
           (PARTITION BY player_name ORDER BY current_season) <> is_active
           OR LAG(is_active, 1) OVER
           (PARTITION BY player_name ORDER BY current_season) IS NULL
           AS did_change
FROM players
),
 streak_identified AS (
     SELECT
        player_name,
            scoring_class,
            is_active,
            current_season,
        SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
            OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
     FROM streak_started
 ),
 aggregated AS (
     SELECT
        player_name,
        scoring_class,
        is_active,
        streak_identifier,
        MIN(current_season) AS start_date,
        MAX(current_season) AS end_date
     FROM streak_identified
     GROUP BY 1,2,3,4
 )

SELECT player_name, scoring_class, is_active, 2022, start_date, end_date
FROM aggregated

-- SCD Year to Year

WITH last_season_scd AS (
  SELECT * FROM players_scd_table
  WHERE current_season = 2020
  AND end_season = 2020
),
historical_scd AS (
  SELECT * FROM players_scd_table
  WHERE current_season = 2020
  AND end_season < 2020
)
this_season AS (
  SELECT * FROM players
  WHERE current_season = 2021
),
unchanged_records AS (
  SELECT ts.player_name, 
    ls.scoring_class, ls.is_active, ls.start_season, ts.current_season as end_season, ts.current_season
    FROM this_season ts
    JOIN last_season_scd ls 
    ON ts.player_name = ls.player_name
    WHERE ts.scoring_class = ls.scoring_class 
    AND ts.is_active = ls.is_active
  )


select ts.player_name, 
  ls.scoring_class, ts.is_active, 
  ls.scoring_class, ls.is_active, 
FROM this_season ts
  LEFT JOIN last_season_scd ls 
  ON ts.player_name = ls.player_name
