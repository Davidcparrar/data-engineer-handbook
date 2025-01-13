CREATE TABLE fct_game_details (
	dim_game_date DATE ,
	dim_season INTEGER, 
	dim_team_id INTEGER, 
	dim_player_id INTEGER,
	dim_player_name TEXT,
	dim_is_playing_at_home BOOLEAN,
	dim_start_position TEXT,
	dim_did_not_play BOOLEAN,
	dim_did_not_dress BOOLEAN,
	dim_not_with_team BOOLEAN,
	m_minutes REAL,
	m_fgm INTEGER,
	m_fga INTEGER,
	m_fg3m INTEGER,
	m_fg3a INTEGER,
	m_ftm INTEGER,
	m_fta INTEGER,
	m_oreb INTEGER,
	m_dreb INTEGER,
	m_reb INTEGER,
	m_ast INTEGER,
	m_stl INTEGER,
	m_blk INTEGER,
	m_turnovers INTEGER,
	m_pf INTEGER,
	m_pts INTEGER,
	m_plus_minus INTEGER,
	PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
)

INSERT INTO fct_game_details
WITH deduped AS (
	SELECT 
		g.game_date_est, 
		g.season, 
		g.home_team_id, 
		gd.*, ROW_NUMBER() OVER (PARTITION BY gd.game_id, gd.team_id, gd.player_id ORDER BY g. game_date_est) AS row_num
	FROM GAME_DETAILS gd
	JOIN games g  
ON g.game_id = gd.game_id
)
SELECT 
	game_date_est,
	season, 
	team_id, 
	player_id,
	player_name,
	team_id = home_team_id AS dim_is_playing_at_home,
	start_position,
	COALESCE(POSITION('DNP' IN "comment"),0) > 0 AS dim_did_not_play,
	COALESCE(POSITION('DND' IN "comment"),0) > 0 AS dim_did_not_dress,
	COALESCE(POSITION('NWT' IN "comment"),0) > 0 AS dim_not_with_team,
	(SPLIT_PART("min", ':', 1)::REAL + SPLIT_PART("min", ':', 2)::REAL/60.0) AS minutes,
	fgm,
	fga,
	fg3m,
	fg3a,
	ftm,
	fta,
	oreb,
	dreb,
	reb,
	ast,
	stl,
	blk,
	"TO" AS turnovers,
	pf,
	pts,
	plus_minus
FROM deduped
WHERE ROW_NUM = 1
