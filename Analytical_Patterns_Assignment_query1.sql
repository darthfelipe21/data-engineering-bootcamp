-- A query that does state change tracking for players
    -- A player entering the league should be New
    -- A player leaving the league should be Retired
    -- A player staying in the league should be Continued Playing
    -- A player that comes out of retirement should be Returned from Retirement
    -- A player that stays out of the league should be Stayed Retired
WITH player_activity AS (
    SELECT
        player_name,
        start_season,
        end_season,
        is_active,
        LAG(is_active, 1, FALSE) OVER (PARTITION BY player_name ORDER BY start_season) AS prev_is_active,
        MIN(start_season) OVER (PARTITION BY player_name) AS first_season_for_player
    FROM players_scd
)
SELECT
    player_name,
    start_season,
    CASE
        WHEN start_season = first_season_for_player AND is_active = TRUE THEN 'New'
        WHEN is_active = FALSE AND prev_is_active = TRUE THEN 'Retired'
        WHEN is_active = TRUE AND prev_is_active = FALSE THEN 'Returned from Retirement'
        WHEN is_active = TRUE AND prev_is_active = TRUE THEN 'Continued Playing'
        WHEN is_active = FALSE AND prev_is_active = FALSE THEN 'Stayed Retired'
        ELSE 'Unknown Status' 
    END AS player_status
FROM player_activity
ORDER BY player_name, start_season;