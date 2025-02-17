-- DDL for the graph database

DROP TYPE vertex_type CASCADE;

CREATE TYPE vertex_type AS ENUM('player', 'team', 'game');


CREATE TABLE vertices (
	identifier TEXT,
	type vertex_type,
	properties JSON,
	PRIMARY KEY (identifier, type)
)

CREATE TYPE edge_type AS ENUM('plays_against', 'shares_team', 'plays_in', 'plays_on');

CREATE TABLE edges (
-- edge_id TEXT (Sometimes) -> Primary key
	subject_identifier TEXT, 
	subject_type vertex_type,
	object_identifier TEXT,
	object_type vertex_type,
	edge_type edge_type,
	properties JSON,
	PRIMARY KEY (subject_identifier, subject_type, object_identifier, object_type, edge_type)
) ;


-- Inserting data

-- games
INSERT INTO vertices
SELECT 
	game_id AS identifier,
	'game'::vertex_type AS TYPE,
	json_build_object(
		'pts_home', pts_home,
		'pts_away', pts_away,
		'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
	) AS properties
FROM games;

-- players
INSERT INTO vertices
WITH players_agg AS (
SELECT 
	player_id AS identifier,
	MAX(player_name) AS player_name,
	COUNT(1) AS number_of_games,
	SUM(pts) AS total_points,
	Array_agg(DISTINCT team_id) AS teams
	
FROM game_details
GROUP BY player_id
)

SELECT 
	identifier, 
	'player'::vertex_type, 
	json_build_object(
		'player_name', player_name, 
		'number_of_games', number_of_games,
		'total_points', total_points,
		'teams', teams
	)
FROM players_agg

-- teams
INSERT INTO vertices
WITH teams_deduped AS (
	SELECT *,  ROW_NUMBER() OVER(PARTITION BY team_id) AS row_num -- There are dupes we need to dedupe by team_id
	FROM teams
)
SELECT
	team_id AS identifier,
	'team'::vertex_type AS TYPE,
	json_build_object(
		'abbreviation', abbreviation,
		'nickname', nickname,
		'city', city,
		'arena', arena,
		'year_founded', yearfounded
	)
FROM teams_deduped
WHERE row_num = 1;

-- edges

INSERT INSERT INTO edges
WITH edges_deduped AS (
	SELECT *,  ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS row_num
	FROM game_details
)
SELECT 
	player_id AS subject_identifier,
	'player'::vertex_type AS subject_type,
	game_id AS object_identifier,
	'game'::vertex_type AS obiject_type,
	'plays_in'::edge_type AS edge_type,
	json_build_object(
		'start_position', start_position,
		'pts', pts,
		'team_id', team_id,
		'team_abbreviation', team_abbreviation
	) AS properties
FROM edges_deduped
WHERE row_num = 1;

INSERT INTO edges
WITH games_deduped AS (
	SELECT *,  ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS row_num
	FROM game_details
),
filtered AS (
	SELECT *
	FROM games_deduped
	WHERE row_num = 1
),
aggregated AS (
	SELECT 
		f1.player_id AS subject_player_id, 
		MIN(f1.player_name) AS subject_player_name, -- In case player changed names Not needed in the final insert TBH
		f2.player_id AS object_player_id, 
		MIN(f2.player_name) AS object_player_name, -- In case player changed names
		CASE 
			WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
			ELSE 'plays_against'::edge_type
		END AS edge_type,
		count(1) AS num_games,
		sum(f1.pts) AS subject_points,
		sum(f2.pts) AS object_points
		
	FROM filtered f1 JOIN filtered f2
	ON f1.game_id = f2.game_id
	AND f1.player_name <> f2.player_name
	WHERE f1.pts IS NOT NULL AND f2.pts IS NOT NULL
	AND f1.player_id > f2.player_id -- Avoids duplicates
	GROUP BY f1.player_id, f2.player_id, 
		CASE 
			WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
			ELSE 'plays_against'::edge_type
		END
)
SELECT 
	subject_player_id AS subject_identifier,
	'player'::vertex_type AS subject_type,
	object_player_id AS object_identifier,
	'player'::vertex_type AS object_type,
	edge_type,
	json_build_object(
		'num_games', num_games,
		'subject_points', subject_points,
		'object_points', object_points
		
	) AS properties
	
FROM aggregated

-- Exploratory queries
WITH games_deduped AS (
	SELECT *,  ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS row_num
	FROM game_details
),
filtered AS (
	SELECT *
	FROM games_deduped
	WHERE row_num = 1
)
SELECT 
	f1.player_id, f1.player_name, 
	f2.player_id, f2.player_name, 
	f1.team_abbreviation, f2.team_abbreviation,
	CASE 
		WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
		ELSE 'plays_against'::edge_type
	END AS edge_type,
	count(1) AS num_games,
	sum(f1.pts) AS left_points,
	sum(f2.pts) AS right_points
	
FROM filtered f1 JOIN filtered f2
ON f1.game_id = f2.game_id
AND f1.player_name <> f2.player_name
WHERE f1.pts IS NOT NULL AND f2.pts IS NOT NULL
AND f1.player_id > f2.player_id -- Avoids duplicates
GROUP BY f1.player_id, f2.player_id, 
	f1.player_name, f2.player_name, f1.team_abbreviation, f2.team_abbreviation
ORDER BY f1.team_abbreviation, f2.team_abbreviation;

SELECT TYPE, count(1)
FROM vertices
GROUP BY 1

SELECT 
	v.properties ->> 'player_name' AS player_name,
	MAX(COALESCE((e.properties->>'pts')::INTEGER, 0)::integer) AS points -- There are nulls that make the return be a string
FROM vertices v
JOIN edges e
ON e.subject_identifier = v.identifier
AND e.subject_type = v.type 
GROUP BY player_name
ORDER BY points DESC;

-- How many edges of each type do we have?
SELECT edge_type, count(1) FROM edges e GROUP BY edge_type;

-- exploring edges and vertices
SELECT * 
FROM vertices v JOIN edges e
	ON v.identifier = e.subject_identifier 
	AND v."type" = e.subject_type 
WHERE e.object_type = 'player'::vertex_type

-- exploring scoring vs rivals

SELECT 
	v.properties->>'player_name' player_name,
	e.object_identifier AS rival,
	v2.properties->>'player_name' player_name_rival,
	(v.properties->>'number_of_games')::REAL/
	CASE WHEN (v.properties->>'total_points')::REAL = 0 THEN 1.0
		ELSE (v.properties->>'total_points')::REAL 
	END AS average,
	(e.properties->>'subject_points')::REAL/
	(e.properties->>'num_games')::REAL AS average_vs_rival
FROM vertices v JOIN edges e
	ON v.identifier = e.subject_identifier 
	AND v."type" = e.subject_type 
LEFT JOIN vertices v2 
	ON e.object_identifier = v2.identifier
WHERE e.object_type = 'player'::vertex_type
