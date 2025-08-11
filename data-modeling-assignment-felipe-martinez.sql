-- A query to deduplicate game_details from Day 1 so there's no duplicates
WITH deduplicated AS(
    SELECT *, ROW_NUMBER() OVER(PARTITION BY game_id) AS uniq
    FROM game_details)
SELECT * 
FROM deduplicated
WHERE uniq = 1;


-- A DDL for an user_devices_cumulated table that has:
-- a device_activity_datelist which tracks a users active days by browser_type
-- data type here should look similar to MAP<STRING, ARRAY[DATE]>
    -- or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)
CREATE TABLE user_devices_cumulated (
  user_id NUMERIC NOT NULL,
  device_activity_datelist JSON NOT NULL,
  PRIMARY KEY (user_id)
);

-- A cumulative query to generate device_activity_datelist from events
INSERT INTO user_devices_cumulated
WITH cummulate_browser AS (
  SELECT 
    e.user_id,
    d.browser_type,
    ARRAY_AGG(DISTINCT CAST(e.event_time AS TIMESTAMP) ORDER BY CAST(e.event_time AS TIMESTAMP)) AS date_list
  FROM devices d
  JOIN events e ON d.device_id = e.device_id
  WHERE e.event_time IS NOT NULL AND e.user_id IS NOT NULL
  GROUP BY e.user_id, d.browser_type
)
SELECT 
  user_id,
  json_object_agg(browser_type, date_list) AS device_activity_datelist
FROM cummulate_browser
GROUP BY user_id;

-- A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column
WITH browser_dates_int AS (
  SELECT 
    user_id,
    browser_type,
    ARRAY_AGG(
      TO_CHAR(date_value::TIMESTAMP, 'YYYYMMDD')::INT
      ORDER BY date_value::TIMESTAMP
    ) AS list_int_date
  FROM (
    SELECT 
      user_id,
      key AS browser_type,
      json_array_elements_text(value) AS date_value
    FROM user_devices_cumulated,
         json_each(device_activity_datelist)
  ) AS extracted
  GROUP BY user_id, browser_type
)
SELECT 
  user_id,
  json_object_agg(browser_type, list_int_date) AS datelist_int
FROM browser_dates_int
GROUP BY user_id;

-- A DDL for hosts_cumulated table
CREATE TABLE hosts_cumulated (
  host TEXT,
  host_datelist DATE[],
  PRIMARY KEY (host)
);

-- The incremental query to generate host_activity_datelist
INSERT INTO hosts_cumulated
WITH new_date AS (
  SELECT 
    host,
    CAST(event_time AS TIMESTAMP) AS activity_date
  FROM events
  WHERE event_time IS NOT NULL AND host IS NOT NULL
  GROUP BY host, CAST(event_time AS TIMESTAMP)
),
aggregated AS (
  SELECT 
    host,
    ARRAY_AGG(activity_date ORDER BY activity_date) AS new_datelist
  FROM new_date
  GROUP BY host
),
fusions AS (
  SELECT 
    a.host,
    ARRAY(
      SELECT DISTINCT unnest(
        COALESCE(hc.host_datelist, '{}') || a.new_datelist
      )
    ) AS updated_datelist
  FROM aggregated a
  FULL OUTER JOIN hosts_cumulated hc ON a.host = hc.host
)
SELECT host, updated_datelist
FROM fusions
ON CONFLICT (host) DO UPDATE
SET host_datelist = EXCLUDED.host_datelist;


-- A monthly, reduced fact table DDL host_activity_reduced
    -- month
    -- host
    -- hit_array - think COUNT(1)
    -- unique_visitors array - think COUNT(DISTINCT user_id)
CREATE TABLE host_activity_reduced (
  month DATE NOT NULL,
  host TEXT NOT NULL,
  hit_array NUMERIC[],
  unique_visitors_array NUMERIC[],
  PRIMARY KEY (month, host)
);

-- An incremental query that loads host_activity_reduced
    -- day-by-day
WITH daily_stats AS (
  SELECT 
    DATE_TRUNC('month', event_time::DATE) AS month,
    host,
    EXTRACT(DAY FROM event_time::DATE)::INT AS day,
    COUNT(*) AS hits,
    COUNT(DISTINCT user_id) AS unique_visitors
  FROM events
  WHERE event_time IS NOT NULL AND host IS NOT NULL AND user_id IS NOT NULL
  GROUP BY month, host, day
),
aggregated AS (
  SELECT 
    month,
    host,
    ARRAY_AGG(hits ORDER BY day) AS hit_array,
    ARRAY_AGG(unique_visitors ORDER BY day) AS unique_visitors_array
  FROM daily_stats
  GROUP BY month, host
)
INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors_array)
SELECT month, host, hit_array, unique_visitors_array
FROM aggregated
ON CONFLICT (month, host) DO UPDATE
SET hit_array = EXCLUDED.hit_array,
    unique_visitors_array = EXCLUDED.unique_visitors_array;
