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
