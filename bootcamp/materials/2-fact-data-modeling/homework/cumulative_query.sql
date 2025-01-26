-- Purpose: This query is used to calculate the cumulative number of devices used by each user for each browser type
-- The query must be run for each day to get the cumulative number of devices used by each user for each browser type
INSERT INTO user_devices_cumulated
WITH yesterday AS (
	SELECT * FROM user_devices_cumulated udc
	WHERE date = '2023-01-04'
), today AS ( 
	SELECT e.user_id, d.browser_type, CAST(e.event_time AS DATE) date
	FROM events e JOIN devices d
	ON e.device_id = d.device_id
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

