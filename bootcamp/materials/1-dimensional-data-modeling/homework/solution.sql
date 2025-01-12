-- Task 1
CREATE TYPE films AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	film_id TEXT,
  year INTEGER
);

CREATE TYPE quality_class AS ENUM('star', 'good', 'average', 'bad');

CREATE TABLE actors(
	actor TEXT,
	films films[],
	quality_class quality_class,
	is_active BOOLEAN,
	current_year INTEGER
);

-- Task 2
-- Single Insert Callable form an ETL

INSERT INTO actors
WITH last_year AS (
    SELECT * FROM actors
    WHERE current_year = 1970
),
this_year AS (
    SELECT 
    	actor, 
	    ARRAY_AGG(
    		ROW(film, votes, rating, filmid, year)::films
	    ) films,
	    AVG(rating) average_rating,
	    MAX("year") current_year
	FROM actor_films
    WHERE year = 1971
    GROUP BY actor
)
SELECT 
    COALESCE(t.actor, l.actor) AS actor,
    CASE 
        WHEN l.films IS NULL THEN t.films
        WHEN t.films IS NOT NULL THEN 
            l.films || t.films
        ELSE 
            l.films
    END AS films,
    CASE 
        WHEN t.current_year IS NOT NULL THEN 
            CASE 
                WHEN t.average_rating > 8 THEN 'star'::quality_class
                WHEN t.average_rating> 7 THEN 'good'::quality_class
                WHEN t.average_rating > 6 THEN 'average'::quality_class
                ELSE 'bad'::quality_class
            END
        ELSE 
            l.quality_class
    END AS quality_class,
    CASE 
        WHEN t.current_year IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS is_active,
    COALESCE(t.current_year, l.current_year + 1) AS current_year
FROM this_year t
FULL OUTER JOIN last_year l
ON t.actor = l.actor;


-- Insert all actors from historical actor_films table

INSERT INTO actors
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1970, 2021) AS year
), actors_start_year AS (
	SELECT actor,
		MIN(year) AS start_year
	FROM actor_films
	WHERE "year" < (SELECT MAX("year") FROM years)
	GROUP BY actor
), actors_years AS (
	SELECT a.actor, y."year", a.start_year
	FROM actors_start_year a 
	LEFT JOIN years y
	ON a.start_year <= y."year"
), windowed AS (
	SELECT ay.actor,
	ay."year",
	ay.start_year,
	ARRAY_REMOVE( -- Remove NULL entries FOR years WHERE NO movies WHERE made
		ARRAY_AGG(
			CASE WHEN af.actor IS NOT NULL THEN
				ROW(film, votes, rating, filmid, af."year")::films
			END
		)
		OVER(PARTITION BY ay.actor ORDER BY COALESCE(ay."year", af."year")),
		NULL -- ARRAY_REMOVE works ON non emtpy arrays, calling this OVER films[] fails, ergo films[null]
	) AS films,
	ROW_NUMBER() OVER(PARTITION BY ay.actor, ay."year") AS duplicate -- TO DELETE duplicated ROWS
	FROM actors_years ay
	LEFT JOIN actor_films af
	ON af.actor = ay.actor 
	AND af."year" = ay."year"
	ORDER BY ay.actor, ay."year"	
), static AS (
	SELECT actor, 
	    AVG(rating) average_rating,
	    MAX("year") "year"
	FROM actor_films
	GROUP BY actor, "year"
), merged AS (
	SELECT 
		w.actor, 
		w."year",
		w.start_year,
		w.films,
		average_rating,
		w.films[CARDINALITY(w.films)]."year" = w."year" AS is_active,
		CASE WHEN average_rating IS NOT NULL THEN
			CASE WHEN average_rating > 8 THEN 'star'::quality_class 
			WHEN average_rating > 7 THEN 'good'::quality_class 
			WHEN average_rating > 6 THEN 'average'::quality_class 
			ELSE 'bad'::quality_class
			END
		END AS quality_class
	FROM windowed w
	LEFT JOIN "static" s
		ON w.actor = s.actor 
		AND w."year" = s."year"
	WHERE w.duplicate = 1
), recursive_filled AS ( -- Needed FOR FORWARD filling quality_class AND is_active
	WITH RECURSIVE recursive_filled_cte AS (
	    -- Initialize the first row for each actor
	SELECT 
	   m.actor,
	   m."year",
	   m.films,
	   m.average_rating,
	   m.is_active,
	   m.quality_class
	   
	FROM merged m
	WHERE m."year" = m.start_year
	
	UNION ALL
	
	-- Recursively propagate the last known non-NULL value
	SELECT 
		r.actor,
	   	r."year",
	   	r.films,
	   	COALESCE(r.average_rating, rf.average_rating),
	   	r.is_active,
	   	COALESCE(r.quality_class, rf.quality_class)
	FROM merged r
	JOIN recursive_filled_cte rf
	ON r.actor = rf.actor AND r."year" = rf."year" + 1    
	)
	SELECT * FROM recursive_filled_cte
)
SELECT actor, films, quality_class, is_active, "year" AS current_year FROM recursive_filled
ORDER BY actor, "year";

-- Task 3
-- Create SCD table
CREATE TABLE actors_history_scd (
	actor TEXT,
	is_active BOOLEAN,
	quality_class quality_class,
	start_date INTEGER,
	end_date INTEGER
);

-- Task 4
-- Insert SCD into actors_history_scd
INSERT INTO actors_history_scd
WITH changed AS (
	SELECT actor, is_active, quality_class, current_year,
		LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) <> is_active 
		OR LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) IS NULL
		OR LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) <> quality_class
		OR LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) IS NULL
		AS did_change
		FROM actors
), streak AS (
	SELECT 
	actor,
	is_active,
	quality_class,
	current_year,
	did_change,
	SUM(CASE WHEN did_change THEN 1 ELSE 0 END) OVER (PARTITION BY actor ORDER BY current_year) AS streak
	FROM changed
)
SELECT
	actor,
	is_active,
	quality_class,
	MIN(current_year) AS start_date,
	MAX(current_year) AS end_date
FROM streak
GROUP BY 
actor, is_active, quality_class, streak
ORDER BY actor, start_date;

-- Task 5
-- Incremental Insert into SCD table
-- Helper type
CREATE TYPE scd_actors AS (
	is_active BOOLEAN,
	quality_class quality_class,
	start_date INTEGER,
	end_date INTEGER
);

-- Assuming data is loaded up to 1999
WITH last_year_scd AS (
	SELECT * FROM actors_history_scd
	WHERE end_date = 1999
), historical_scd AS (
	SELECT * FROM actors_history_scd
	WHERE end_date < 1999
), this_year AS (
	SELECT actor, quality_class, is_active, current_year
	FROM actors WHERE current_year = 2000
), unchanged AS (
	SELECT 
		ly.actor,
		ly.is_active,
		ly.quality_class,
		ly.start_date, 
		ly.end_date + 1 AS end_date
	FROM last_year_scd ly
	JOIN this_year ty
	ON ly.actor = ty.actor
	WHERE ly.is_active = ty.is_active
	AND ly.quality_class = ty.quality_class
), changed AS (
	SELECT 
		ly.actor,
		UNNEST(ARRAY[
			ROW(ly.is_active, ly.quality_class, ly.start_date, ly.end_date)::scd_actors,
			ROW(ty.is_active, ty.quality_class, ty.current_year, ty.current_year)::scd_actors
		]) AS changed_records
	FROM last_year_scd ly
	JOIN this_year ty
	ON ly.actor = ty.actor
	WHERE ly.is_active <> ty.is_active
	OR ly.quality_class <> ty.quality_class
), unnested_changed AS (
	SELECT actor,
	(changed_records::scd_actors).is_active,
	(changed_records::scd_actors).quality_class,
	(changed_records::scd_actors).start_date,
	(changed_records::scd_actors).end_date
	FROM changed
),new_records AS (
	SELECT 
		ty.actor,
		ty.is_active,
		ty.quality_class,
		ty.current_year AS start_date,
		ty.current_year AS end_date
	FROM this_year ty 
	LEFT JOIN last_year_scd ly
	ON ty.actor = ly.actor
	WHERE ly.actor IS NULL
)
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged
UNION ALL
SELECT * FROM unnested_changed
UNION ALL
SELECT * FROM new_records;
