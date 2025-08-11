CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    num_hits BIGINT
);


CREATE TABLE processed_events_aggregated_source (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    referrer VARCHAR,
    num_hits BIGINT
);


WITH conteo AS (
    SELECT COUNT(event_hour) AS count_event, host
    FROM processed_events_aggregated
    WHERE host LIKE '%techcreator%'
    GROUP BY host
),
total AS (
    SELECT SUM(count_event) AS total_events
    FROM conteo
)
SELECT 
    c.host,
    c.count_event,
    c.count_event / t.total_events AS pct_event
FROM conteo c
CROSS JOIN total t
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io');


SELECT 
    host,
    AVG(event_count) AS avg_events_per_session
FROM (
    SELECT 
        host,
        COUNT(*) AS event_count
    FROM processed_events_aggregated
    WHERE host LIKE '%techcreator%'
    --WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
    GROUP BY host
) session_events
GROUP BY host;


WITH session_events AS(
    SELECT 
        host,
        COUNT(*) AS event_count
    FROM processed_events_aggregated
    --WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
    GROUP BY host
)
SELECT 
    host,
    AVG(event_count) AS avg_events_per_session
FROM session_events
GROUP BY host;

SELECT * FROM processed_events_aggregated;

SELECT * FROM processed_events_aggregated_source;