-- What is the most games a team has won in a 90 game stretch?
SELECT
    team_abbreviation,
    wins_in_90_games AS max_wins_in_90_game_stretch
FROM (
    SELECT
        team_abbreviation,
        game_id, -- Keep game_id to maintain uniqueness for ordering if necessary
        SUM(CASE WHEN CAST(plus_minus AS INT) > 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY team_abbreviation
            ORDER BY game_id -- Assuming game_id provides chronological order within a team's games
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW -- For a 90-game stretch, it's current row + 89 preceding rows
        ) AS wins_in_90_games
    FROM game_details
) AS team_game_streaks
ORDER BY max_wins_in_90_game_stretch DESC
LIMIT 1;