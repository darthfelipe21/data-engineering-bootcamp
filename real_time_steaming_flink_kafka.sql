-- Esta tabla se creo para llevar los registro de apache flink
CREATE TABLE IF NOT EXISTS processed_events (
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR
);

SELECT geodata::json->>'country',
       COUNT(1)
FROM processed_events
GROUP BY 1
LIMIT 10;

SELECT * FROM processed_events;