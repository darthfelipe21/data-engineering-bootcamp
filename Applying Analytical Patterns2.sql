-- GROUPING SETS
CREATE TABLE web_events_dashboard AS
WITH combined AS (
    SELECT COALESCE(d.browser_type, 'N/A') AS browser_type,
           COALESCE(d.os_type, 'N/A') AS os_type,
           e.*,
           CASE 
                WHEN referrer LIKE '%zachwilson%' THEN 'On-site'
                WHEN referrer LIKE '%eczachly%' THEN 'On-site'
                WHEN referrer LIKE '%dataengineer.io%' THEN 'On-site'
                WHEN referrer LIKE '%t.co%' THEN 'Twitter'
                WHEN referrer LIKE '%linkedin%' THEN 'Linkedin'
                WHEN referrer LIKE '%reddit%' THEN 'Reddit'
                WHEN referrer IS NULL THEN 'Direct'
                ELSE 'Other'
                END AS referrer_mapped
    FROM events e
    JOIN devices d
        ON d.device_id = e.device_id
)
SELECT COALESCE(referrer_mapped, '(overall)') AS referrer,
       COALESCE(browser_type, '(overall)') AS browser_type,
       COALESCE(os_type, '(overall)') AS os_type,
       COUNT(1) AS number_of_site_hits,
       COUNT(CASE WHEN url = '/signup' THEN 1 END) AS number_of_signup_visits,
       COUNT(CASE WHEN url = '/contact' THEN 1 END) AS number_of_contact_visits,
       COUNT(CASE WHEN url = '/login' THEN 1 END) AS number_of_login_visits,
       CAST(COUNT(CASE WHEN url = '/signup' THEN 1 END) AS REAL) / COUNT(1) AS pct_visited_signup
FROM combined GROUP BY GROUPING SETS (
                       (referrer_mapped, browser_type, os_type),
                       (os_type),
                       (browser_type),
                       (referrer_mapped),
                       ()
)
HAVING COUNT(1) > 100
ORDER BY CAST(COUNT(CASE WHEN url = '/signup' THEN 1 END) AS REAL) / COUNT(1) DESC;

SELECT * FROM web_events_dashboard;



-- ROLLUP muy util en data jerarquica
WITH combined AS (
    SELECT COALESCE(d.browser_type, 'N/A') AS browser_type,
           COALESCE(d.os_type, 'N/A') AS os_type,
           e.*,
           CASE 
                WHEN referrer LIKE '%zachwilson%' THEN 'On-site'
                WHEN referrer LIKE '%eczachly%' THEN 'On-site'
                WHEN referrer LIKE '%dataengineer.io%' THEN 'On-site'
                WHEN referrer LIKE '%t.co%' THEN 'Twitter'
                WHEN referrer LIKE '%linkedin%' THEN 'Linkedin'
                WHEN referrer LIKE '%reddit%' THEN 'Reddit'
                WHEN referrer IS NULL THEN 'Direct'
                ELSE 'Other'
                END AS referrer_mapped
    FROM events e
    JOIN devices d
        ON d.device_id = e.device_id
)
SELECT COALESCE(referrer_mapped, '(overall)') AS referrer,
       COALESCE(browser_type, '(overall)') AS browser_type,
       COALESCE(os_type, '(overall)') AS os_type,
       COUNT(1) AS number_of_site_hits,
       COUNT(CASE WHEN url = '/signup' THEN 1 END) AS number_of_signup_visits,
       COUNT(CASE WHEN url = '/contact' THEN 1 END) AS number_of_contact_visits,
       COUNT(CASE WHEN url = '/login' THEN 1 END) AS number_of_login_visits,
       CAST(COUNT(CASE WHEN url = '/signup' THEN 1 END) AS REAL) / COUNT(1) AS pct_visited_signup
FROM combined GROUP BY ROLLUP (referrer_mapped, browser_type, os_type)
HAVING COUNT(1) > 100
ORDER BY CAST(COUNT(CASE WHEN url = '/signup' THEN 1 END) AS REAL) / COUNT(1) DESC;
-- ROLLUP en este caso es lo mismo que un GROUPING SETS ((referrer_mapped), (referrer_mapped, browser_type), (referrer_mapped, browser_type, os_type)) 





-- CUBE 
WITH combined AS (
    SELECT COALESCE(d.browser_type, 'N/A') AS browser_type,
           COALESCE(d.os_type, 'N/A') AS os_type,
           e.*,
           CASE 
                WHEN referrer LIKE '%zachwilson%' THEN 'On-site'
                WHEN referrer LIKE '%eczachly%' THEN 'On-site'
                WHEN referrer LIKE '%dataengineer.io%' THEN 'On-site'
                WHEN referrer LIKE '%t.co%' THEN 'Twitter'
                WHEN referrer LIKE '%linkedin%' THEN 'Linkedin'
                WHEN referrer LIKE '%reddit%' THEN 'Reddit'
                WHEN referrer IS NULL THEN 'Direct'
                ELSE 'Other'
                END AS referrer_mapped
    FROM events e
    JOIN devices d
        ON d.device_id = e.device_id
)
SELECT COALESCE(referrer_mapped, '(overall)') AS referrer,
       COALESCE(browser_type, '(overall)') AS browser_type,
       COALESCE(os_type, '(overall)') AS os_type,
       COUNT(1) AS number_of_site_hits,
       COUNT(CASE WHEN url = '/signup' THEN 1 END) AS number_of_signup_visits,
       COUNT(CASE WHEN url = '/contact' THEN 1 END) AS number_of_contact_visits,
       COUNT(CASE WHEN url = '/login' THEN 1 END) AS number_of_login_visits,
       CAST(COUNT(CASE WHEN url = '/signup' THEN 1 END) AS REAL) / COUNT(1) AS pct_visited_signup
FROM combined GROUP BY CUBE (referrer_mapped, browser_type, os_type)
ORDER BY CAST(COUNT(CASE WHEN url = '/signup' THEN 1 END) AS REAL) / COUNT(1) DESC;



-- SELFJOIN
WITH combined AS (
    SELECT COALESCE(d.browser_type, 'N/A') AS browser_type,
           COALESCE(d.os_type, 'N/A') AS os_type,
           e.*,
           CASE 
                WHEN referrer LIKE '%zachwilson%' THEN 'On-site'
                WHEN referrer LIKE '%eczachly%' THEN 'On-site'
                WHEN referrer LIKE '%dataengineer.io%' THEN 'On-site'
                WHEN referrer LIKE '%t.co%' THEN 'Twitter'
                WHEN referrer LIKE '%linkedin%' THEN 'Linkedin'
                WHEN referrer LIKE '%reddit%' THEN 'Reddit'
                WHEN referrer IS NULL THEN 'Direct'
                ELSE 'Other'
                END AS referrer_mapped
    FROM events e
    JOIN devices d
        ON d.device_id = e.device_id
), aggregated AS (
SELECT c1.user_id, c1.url AS to_url, c2.url AS from_url, MIN(CAST(c1.event_time AS TIME) - CAST(c2.event_time AS TIME)) AS time_to_go_other_page
FROM combined c1 JOIN combined c2
    ON c1.user_id = c2.user_id 
    AND DATE(c1.event_time) = DATE(c2.event_time)
    AND c1.event_time > c2.event_time
GROUP BY c1.user_id, c1.url, c2.url
)
SELECT to_url, from_url, COUNT(1), MIN(time_to_go_other_page), MAX(time_to_go_other_page), AVG(time_to_go_other_page)
FROM aggregated
GROUP BY to_url, from_url
HAVING count(1) > 100;