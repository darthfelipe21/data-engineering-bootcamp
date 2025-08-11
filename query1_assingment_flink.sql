-- Q1: Average number of web events per user session on Tech Creator
-- Counts events per unique session (session_start, ip, host) for 'techcreator' domain
-- Computes average event count across sessions
WITH session_event_counts AS (
    SELECT
        session_start,
        ip,
        host,
        COUNT(*) AS event_count_per_session
    FROM
        assignment_events_sessions
    WHERE
        host LIKE '%techcreator%'
    GROUP BY
        session_start, ip, host
)
SELECT
    AVG(event_count_per_session) AS average_events_per_session_on_techcreator
FROM
    session_event_counts;