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
    -- Two cases need to be handled, one for when the host is not present in the yesterday table 
    -- and one for when it is present. When it is not present, we need to fill the array with 0s
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
ON CONFLICT (host, month_start) -- This is the primary key
DO UPDATE 
SET 
    hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors;
