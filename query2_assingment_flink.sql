-- Q2: Compare average events per session across specified hosts
-- Calculates average events per session for specific hosts
-- Groups by host for direct comparison
WITH specified_host_session_event_counts AS (
    SELECT
        session_start,
        ip,
        host,
        COUNT(*) AS event_count_per_session
    FROM
        assignment_events_sessions
    WHERE
        host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
    GROUP BY
        session_start, ip, host
)
SELECT
    host,
    AVG(event_count_per_session) AS average_events_per_session
FROM
    specified_host_session_event_counts
GROUP BY
    host -- Group by host
ORDER BY
    average_events_per_session DESC;