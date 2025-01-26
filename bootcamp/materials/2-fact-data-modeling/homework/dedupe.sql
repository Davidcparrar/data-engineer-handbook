-- Purpose: remove duplicates based on row number
WITH deduped AS (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY gd.game_id, gd.team_id, gd.player_id) AS row_num
 FROM game_details gd
)
SELECT * FROM deduped WHERE row_num = 1;

-- Same query but with DISTINCT ON (Other SQL dialects support QUALIFY ROW_NUMBER() = 1 instead)
SELECT DISTINCT ON (gd.game_id, gd.team_id, gd.player_id) gd.*
FROM game_details gd;

