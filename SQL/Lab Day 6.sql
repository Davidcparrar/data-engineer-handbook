CREATE TABLE array_metrics (

	user_id NUMERIC, 
	month_start DATE,
	metric_name TEXT,
	metric_array REAL[],
	PRIMARY KEY (user_id, month_start, metric_name)
)


INSERT INTO array_metrics
WITH daily_aggregate AS (
	SELECT
		user_id, count(1) AS num_site_hits,
		date(event_time) AS date
		FROM events
		WHERE date(event_time) = date('2023-01-06')
		AND user_id IS NOT NULL
		GROUP BY user_id, date
), yesterday AS (
	SELECT * FROM array_metrics
	WHERE month_start = date('2023-01-01')
)
SELECT 
	COALESCE(da.user_id , y.user_id) AS user_id,
	COALESCE(y.month_start, date_trunc('month', da.date)) AS month_start,
	'site_hits' AS metric_name,
	CASE WHEN y.metric_array IS NOT NULL 
		THEN y.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)]
	ELSE
		array_fill(0, ARRAY[COALESCE(date - date(date_trunc('month', da.date)), 0)]) || ARRAY[COALESCE(da.num_site_hits,0)]
	END AS metric_array
FROM daily_aggregate da
	FULL OUTER JOIN yesterday y ON
	da.user_id = y.user_id 
ON CONFLICT (user_id, month_start, metric_name)
DO 
	UPDATE SET metric_array = EXCLUDED.metric_array

-- Using ORDINALITY to get the index of the array element (Basically a pivot)
  WITH agg AS( 
	SELECT metric_name, month_start, ARRAY[sum(metric_array[1]), sum(metric_array[2]), sum(metric_array[3])] AS summed_array
	FROM array_metrics
	GROUP BY metric_name, month_start 
)

SELECT metric_name, month_start + CAST(CAST(INDEX - 1 AS text) || 'day' AS INTERVAL),
elem AS value
FROM agg CROSS JOIN UNNEST(agg.summed_array)
WITH ORDINALITY AS a(elem,index)


-- Avoiding hardcoding the number of days in the month
  WITH expanded AS (
    SELECT 
        metric_name,
        month_start,
        generate_series(1, array_length(metric_array, 1)) AS index,
        metric_array[generate_series(1, array_length(metric_array, 1))] AS value
    FROM array_metrics
),
aggregated AS (
    SELECT 
        metric_name,
        month_start,
        index,
        SUM(value) AS summed_value
    FROM expanded
    GROUP BY metric_name, month_start, index
),
result AS (
    SELECT 
        metric_name,
        month_start + (index - 1) * INTERVAL '1 day' AS adjusted_date,
        summed_value
    FROM aggregated
)
SELECT * FROM result
ORDER BY adjusted_date
