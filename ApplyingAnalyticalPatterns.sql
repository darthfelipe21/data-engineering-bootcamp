-- Mastering Growth Accounting and Retention Analysis
CREATE TABLE users_growth_accounting (
    user_id TEXT,
    first_active_date DATE,
    last_active_date DATE,
    daily_active_state TEXT,
    weekly_active_date TEXT,
    dates_active DATE[],
    date DATE,
    PRIMARY KEY (user_id, date)
);


INSERT INTO users_growth_accounting
WITH yesterday AS (
    SELECT * 
    FROM users_growth_accounting
    WHERE date = DATE('2023-01-13')
), today AS (
    SELECT CAST(user_id AS TEXT) AS user_id, 
           DATE_TRUNC('day', event_time::timestamp) AS today_date, 
           COUNT(1)
    FROM events
    WHERE DATE_TRUNC('day', event_time::timestamp) = DATE('2023-01-14') AND user_id IS NOT NULL
    GROUP BY user_id, DATE_TRUNC('day', event_time::timestamp)
)
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(y.first_active_date, t.today_date) AS first_active_date,
    COALESCE(t.today_date, y.last_active_date) AS last_active_date,
    CASE
        WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'New'
        WHEN y.last_active_date = t.today_date - INTERVAL '1 day' THEN 'Retained'
        WHEN y.last_active_date < t.today_date - INTERVAL '1 day' THEN 'Resurrected'
        WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
        ELSE 'Stale'
    END AS daily_active_date,
    CASE
        WHEN y.user_id IS NULL THEN 'New'
        WHEN y.last_active_date >= y.date - INTERVAL '7 day' THEN 'Retained'
        WHEN y.last_active_date < t.today_date - INTERVAL '7 day' THEN 'Resurrected'
        WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
        ELSE 'Stale'
    END AS weekly_active_date,
    COALESCE(
        y.dates_active,
        ARRAY[]::DATE[]) || CASE WHEN
                                    t.user_id IS NOT NULL
                                    THEN ARRAY[t.today_date]
                                    ELSE ARRAY[]::DATE[]
                                    END AS date_list,
    COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id;



SELECT * 
FROM users_growth_accounting 
WHERE date = DATE('2023-01-09');



SELECT date, daily_active_state, COUNT(1)
FROM users_growth_accounting
GROUP BY date, daily_active_state
ORDER BY date ASC;



SELECT date - first_active_date AS days_since_singup,
       EXTRACT(dow FROM first_active_date) AS dow,
       CAST(COUNT(CASE
                      WHEN daily_active_state IN ('Retained', 'Resurrected', 'New') THEN 1 END) AS REAL) / COUNT(1) AS pct_active,
       COUNT(1)
FROM users_growth_accounting
WHERE first_active_date = DATE('2023-01-12')
GROUP BY date - first_active_date, EXTRACT(dow FROM first_active_date);

