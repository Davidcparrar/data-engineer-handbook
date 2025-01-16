CREATE TABLE users_cumulated (
	user_id TEXT,
	-- List of dates in the past where the user was active
	dates_active DATE[],
	-- Current date for the user
	date DATE,
	PRIMARY KEY (user_id, date)	
);

INSERT INTO users_cumulated
WITH yesterday AS (
	SELECT * 
	FROM users_cumulated
	WHERE date = DATE('2023-01-30')	
),
today AS (
	SELECT CAST(user_id AS TEXT) AS user_id,
	DATE(CAST(event_time AS TIMESTAMP)) AS dates_active
	
	FROM events
		WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
		AND user_id IS NOT null
		GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
)
SELECT 
	COALESCE(t.user_id, y.user_id) user_id,  
	CASE  
		WHEN 
			y.dates_active IS NULL THEN ARRAY[t.dates_active]
		WHEN 
			t.dates_active IS NULL THEN y.dates_active
		ELSE
			ARRAY[t.dates_active] || y.dates_active 
	END dates_active,
	COALESCE(t.dates_active, y.date + INTERVAL '1 day') AS date
FROM today t FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id

-- daltelist active users
WITH users AS (
	SELECT * FROM users_cumulated 
	WHERE date = DATE('2023-01-31')
), series AS (
	SELECT * FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY')
	AS series_date	
), placeholder_int AS (
	SELECT 
	CASE WHEn dates_active @> ARRAY[DATE(series_date)]
		THEN POW(2, 32 - (date - DATE(series_date)))
		ELSE 0
		END AS placeholder_int_value
	,*
	FROM users CROSS JOIN series 
)
SELECT
	user_id,
	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int, 
	BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active,
  -- Like this the pattern may be changed to get different derived dimensions
	BIT_COUNT(CAST('1111111000000000000000000000000' AS BIT(32)) AS dim_is_weekly_active
FROM placeholder_int	
GROUP BY user_id
