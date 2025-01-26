-- Purpose: Remove duplicates based on row number
WITH deduped AS (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY gd.game_id, gd.team_id, gd.player_id) AS row_num
 FROM game_details gd
)
SELECT * FROM deduped WHERE row_num = 1;

-- Same query but with DISTINCT ON (Other SQL dialects support QUALIFY ROW_NUMBER() = 1 instead)
SELECT DISTINCT ON (gd.game_id, gd.team_id, gd.player_id) gd.*
FROM game_details gd;

-- Purpose: Create the table user_devices_cumulated.
CREATE TABLE user_devices_cumulated (
	user_id NUMERIC, 
	device_id NUMERIC,
	browser_type TEXT,
	device_activity_datelist INTEGER[],
	PRIMARY KEY (user_id, device_id, browser_type)
);

-- Purpose: This query is used to calculate the cumulative number of devices used by each user for each browser type
-- The query must be run for each day to get the cumulative number of devices used by each user for each browser type
INSERT INTO user_devices_cumulated
WITH yesterday AS (
	SELECT * FROM user_devices_cumulated udc
  -- This is updated every day to get the previous day's cumulative view
	WHERE date = '2023-01-04'
), today AS ( 
	SELECT e.user_id, d.browser_type, CAST(e.event_time AS DATE) date
	FROM events e JOIN devices d
	ON e.device_id = d.device_id
  -- Modify this day sequentially to load the corresponding data
	WHERE CAST(event_time AS DATE) = DATE('2023-01-05')
	AND user_id IS NOT NULL 
	GROUP BY e.user_id, d.browser_type, CAST(e.event_time AS DATE)
)
SELECT 
	COALESCE(y.user_id, t.user_id) user_id,
	COALESCE(y.browser_type, t.browser_type) browser_type,
	CASE 
  -- If there are no devices used by the user on the previous day, then the 
  -- device_activity_datelist is the array of the current date 
	WHEN y.date IS NULL THEN
		ARRAY[t.date]
	WHEN t.date IS NULL THEN
		y.device_activity_datelist
	ELSE 
		y.device_activity_datelist || ARRAY[t.date]
	END AS device_activity_datelist,
	CAST(COALESCE(t.date, y.date + INTERVAL '1 DAY') AS DATE) date
FROM today t 
FULL OUTER JOIN yesterday y 
ON t.user_id = y.user_id AND t.browser_type = y.browser_type 

-- Purpose: Generate a datelist_int from a datelist_activity_datelist
WITH user_devices AS (
	SELECT * FROM user_devices_cumulated
  -- We use the last day of the month to get the monthly view because it has all the data
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

-- Purpose: Create the table host_cumulated.
CREATE TABLE host_cumulated(
	host TEXT, 
	host_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY (host, date)
);

-- Purpose: Load the host_cumulated table with the host activity datelist

INSERT INTO host_cumulated
WITH yesterday AS (
	SELECT * FROM host_cumulated
	WHERE date = '2023-01-02' -- Set for every day 
), today AS ( 
	SELECT e.host, CAST(e.event_time AS DATE) date
	FROM events e 
	WHERE CAST(event_time AS DATE) = DATE('2023-01-03') -- Always 1 day after yesterday
	AND user_id IS NOT NULL 
	GROUP BY e.host, CAST(e.event_time AS DATE)
)
SELECT 
	COALESCE(y.host, t.host) host,
	CASE 
	WHEN y.date IS NULL THEN
		ARRAY[t.date]
	WHEN t.date IS NULL THEN
		y.host_activity_datelist
	ELSE 
		y.host_activity_datelist || ARRAY[t.date]
	END AS host_activity_datelist,
	CAST(COALESCE(t.date, y.date + INTERVAL '1 DAY') AS DATE) date
FROM today t 
FULL OUTER JOIN yesterday y 
ON t.host = y.host 

-- Purpose: DDL for host_activity_reduced table
CREATE TABLE host_activity_reduced ( 
	host TEXT,
	month_start DATE,
	hit_array INTEGER[],
	unique_visitors INTEGER[],
	PRIMARY KEY (host, month_start)
);

-- Purpose: Load the host_activity_reduced table from the event table
-- this query needs to be run sequentially for each day of the month

INSERT INTO host_activity_reduced
WITH yesterday AS (
    SELECT * 
    FROM host_activity_reduced
    -- This is updated every month to get the month's reduced view 
    WHERE month_start = '2023-01-01'
), 
today AS (
    SELECT 
        COUNT(1) AS hits, 
        host, 
        DATE(event_time) AS date,  
        COUNT(DISTINCT user_id) AS unique_visitors, 
        DATE_TRUNC('MONTH', event_time::DATE)::DATE AS month_start 
    FROM events
    -- Modify this day sequentially to load the corresponding data
    WHERE DATE(event_time) = DATE('2023-01-07')
    GROUP BY DATE(event_time), host
)
SELECT 
    COALESCE(t.host, y.host) AS host,
    COALESCE(t.month_start, y.month_start) AS month_start,
    CASE 
        WHEN y.host IS NOT NULL THEN y.hit_array || ARRAY[COALESCE(t.hits, 0)]
        ELSE ARRAY_FILL(0, ARRAY[COALESCE(t.date - t.month_start,0)]) || ARRAY[t.hits] 
    END AS hit_array,
    CASE 
        WHEN y.host IS NOT NULL THEN y.unique_visitors || ARRAY[COALESCE(t.unique_visitors, 0)]
        ELSE ARRAY_FILL(0, ARRAY[COALESCE(t.date - t.month_start,0)]) || ARRAY[t.unique_visitors] 
    END AS unique_visitors
FROM yesterday y 
FULL OUTER JOIN today t
ON y.host = t.host
ON CONFLICT (host, month_start)
DO UPDATE 
SET 
    hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors;
