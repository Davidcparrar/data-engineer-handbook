-- Purpose: Generate a datelist_int from a datelist_activity_datelist
WITH user_devices AS (
	SELECT * FROM user_devices_cumulated
	WHERE date = '2023-01-31'
), series AS (
  -- Generate series of dates for cross join
	SELECT * FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS serie_date
), placeholder AS (
	SELECT 
	CASE WHEN device_activity_datelist @> ARRAY[DATE(serie_date)]
    -- Get the integer value of each date as a power of 2
		THEN POW(2, 32 - (date - DATE(serie_date)))
		ELSE 0
		END AS placeholder_int_value
	,*
	FROM user_devices CROSS JOIN series s
)
SELECT 
	user_id,
	browser_type,
	date,
  -- Sum the integer values of the dates to get the datelist_int as a bit(32)
	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int
FROM placeholder	
GROUP BY user_id, browser_type, date
