-- How many games in a row did LeBron James score over 10 points a game?
WITH player_scoring AS (
    SELECT
        player_name,
        game_id,
        CAST(pts AS INT) AS points_scored,
        ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY game_id) AS rn
    FROM game_details
    WHERE player_name = 'LeBron James'
),
streaks_identified AS (
    SELECT
        player_name,
        game_id,
        points_scored,
        rn - ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY game_id) AS streak_group_id
    FROM player_scoring
    WHERE points_scored > 10
)
SELECT
    player_name,
    COUNT(game_id) AS longest_streak_with_10_or_more_pts
FROM streaks_identified
GROUP BY player_name, streak_group_id
ORDER BY longest_streak_with_10_or_more_pts DESC
LIMIT 1;