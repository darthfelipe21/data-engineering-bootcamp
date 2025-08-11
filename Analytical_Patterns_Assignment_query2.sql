-- A query that uses GROUPING SETS to do efficient aggregations of game_details data
    -- Aggregate this dataset along the following dimensions
        -- player and team
            -- Answer questions like who scored the most points playing for one team?
        -- player and season
            -- Answer questions like who scored the most points in one season?
        -- team
            -- Answer questions like which team has won the most games?
WITH aggregated_points AS (
  SELECT
    ps.season,
    gd.player_name,
    gd.team_abbreviation,
    SUM(CAST(gd.pts AS INT)) AS total_points,
    GROUPING(gd.player_name) AS is_player_agg,
    GROUPING(gd.team_abbreviation) AS is_team_agg
  FROM game_details gd
  JOIN player_seasons ps
    ON gd.player_name = ps.player_name
  GROUP BY GROUPING SETS (
    (ps.season, gd.player_name, gd.team_abbreviation),
    (ps.season, gd.player_name),                    
    (ps.season, gd.team_abbreviation),              
    (ps.season),                                    
    ()                                          
  )
)
SELECT
  season,
  player_name,
  team_abbreviation,
  total_points,
  CASE
    WHEN is_player_agg = 0 AND is_team_agg = 0 AND season IS NOT NULL THEN
        RANK() OVER (PARTITION BY season, player_name, team_abbreviation ORDER BY total_points DESC)
    WHEN is_player_agg = 0 AND is_team_agg = 1 AND season IS NOT NULL THEN
        RANK() OVER (PARTITION BY season, player_name ORDER BY total_points DESC)
    WHEN is_player_agg = 1 AND is_team_agg = 0 AND season IS NOT NULL THEN
        RANK() OVER (PARTITION BY season, team_abbreviation ORDER BY total_points DESC)
    ELSE NULL
  END AS rank_in_group
FROM aggregated_points
WHERE season IS NOT NULL OR (season IS NULL AND player_name IS NULL AND team_abbreviation IS NULL)
ORDER BY season NULLS LAST, total_points DESC, player_name NULLS LAST, team_abbreviation NULLS LAST;