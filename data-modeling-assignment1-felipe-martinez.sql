-- DDL for actors table: Create a DDL for an actors table with the following fields:
    -- films: An array of struct with the following fields:
		-- film: The name of the film.
		-- votes: The number of votes the film received.
		-- rating: The rating of the film.
		-- filmid: A unique identifier for each film.
CREATE TYPE quality_class AS ENUM('star', 'good', 'average', 'bad');

CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TABLE actors(
    actor TEXT,
    year INTEGER,
    films films[],
    quality_class quality_class,
    current_year INTEGER,
    is_active BOOLEAN
);

-- Cumulative table generation query: Write a query that populates the actors table one year at a time.
INSERT INTO actors
WITH past_year AS (
    SELECT * FROM actors
    WHERE year = 1973  -- Incremental 1 by 1, for every execution, until 2020
),
next_year AS (
    SELECT *
    FROM actor_films
    WHERE year = 1974 -- Incremental 1 by 1, for every execution, until 2021
),
next_year_agg AS (
    SELECT
        actor,
        year,
        ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS new_films,
        AVG(rating) AS avg_rating
    FROM next_year
    GROUP BY actor, year
)
SELECT
    COALESCE(nya.actor, py.actor) AS actor,
    COALESCE(nya.year, py.year + 1) AS year,
    CASE
        WHEN py.films IS NULL THEN nya.new_films
        WHEN nya.new_films IS NULL THEN py.films
        ELSE py.films || nya.new_films
    END AS films,
    CASE
        WHEN nya.avg_rating > 8 THEN 'star'
        WHEN nya.avg_rating > 7 AND nya.avg_rating <= 8 THEN 'good'
        WHEN nya.avg_rating > 6 AND nya.avg_rating <= 7 THEN 'average'
        ELSE 'bad'
    END::quality_class,
    COALESCE(nya.year, py.year + 1) AS current_year,
    nya.new_films IS NOT NULL AS is_active
FROM past_year py
FULL OUTER JOIN next_year_agg nya ON nya.actor = py.actor;


-- DDL for actors_history_scd table: Create a DDL for an actors_history_scd table with the following features:
    -- Implements type 2 dimension modeling (i.e., includes start_date and end_date fields).
    -- Tracks quality_class and is_active status for each actor in the actors table.
CREATE TABLE actors_history_scd (
    actor TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER,
    PRIMARY KEY (actor, start_date)
);


-- Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
INSERT INTO actors_history_scd
WITH with_previous AS(
SELECT actor, 
       current_year,
       quality_class, 
       is_active, 
       LAG(quality_class,1) OVER (PARTITION BY actor ORDER BY current_year) AS previous_quality_class,
       LAG(is_active,1) OVER (PARTITION BY actor ORDER BY current_year) AS previous_is_active
FROM actors
       ),
     with_indicators AS (
SELECT *, 
CASE WHEN quality_class <> previous_quality_class THEN 1
     WHEN is_active <> previous_is_active THEN 1 
     ELSE 0 
     END AS change_indicator
FROM with_previous
WHERE current_year <= 2021
),
    with_backfill AS (
SELECT *, SUM(change_indicator) OVER (PARTITION BY actor ORDER BY current_year) AS backfill_identifier
FROM with_indicators  
)
SELECT actor, quality_class, is_active, MIN(current_year) AS start_date, MAX(current_year) AS end_date, 2021 AS current_year
FROM with_backfill
GROUP BY actor, is_active, quality_class
ORDER BY actor;

-- Incremental query for actors_history_scd: Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table.
WITH past_year_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE current_year = 2021 AND end_date = 2021
),
historical_scd AS (
    SELECT actor, quality_class, is_active, start_date, end_date, current_year
    FROM actors_history_scd
    WHERE current_year = 2021 AND end_date < 2021
),
this_year AS (
    SELECT *
    FROM actors
    WHERE current_year = 2022
),
unchanged_records AS (
    SELECT 
        ts.actor, 
        ts.quality_class, 
        ts.is_active, 
        py.start_date, 
        ts.current_year AS end_date,
        ts.current_year
    FROM this_year ts
    JOIN past_year_scd py 
        ON py.actor = ts.actor
        AND ts.quality_class = py.quality_class
        AND ts.is_active = py.is_active
),
changed_records AS (
    SELECT 
        ty.actor,
        ty.quality_class,
        ty.is_active,
        py.start_date,
        2021 AS end_date,
        py.current_year
    FROM this_year ty
    JOIN past_year_scd py 
        ON py.actor = ty.actor
    WHERE ty.quality_class <> py.quality_class
       OR ty.is_active <> py.is_active
    UNION ALL
    SELECT 
        ty.actor,
        ty.quality_class,
        ty.is_active,
        ty.current_year AS start_date,
        ty.current_year AS end_date,
        ty.current_year
    FROM this_year ty
    JOIN past_year_scd py 
        ON py.actor = ty.actor
    WHERE ty.quality_class <> py.quality_class
       OR ty.is_active <> py.is_active
),
new_records AS (
    SELECT 
        ty.actor,
        ty.quality_class,
        ty.is_active,
        ty.current_year AS start_date,
        ty.current_year AS end_date,
        ty.current_year
    FROM this_year ty
    LEFT JOIN past_year_scd py 
        ON ty.actor = py.actor
    WHERE py.actor IS NULL
)
SELECT *
FROM historical_scd
UNION ALL 
SELECT *
FROM unchanged_records
UNION ALL
SELECT *
FROM changed_records
UNION ALL
SELECT *
FROM new_records
ORDER BY actor, start_date;

