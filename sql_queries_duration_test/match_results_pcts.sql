-- match results pcts - window function
CREATE OR REPLACE VIEW v_match_results_pcts_league_season AS (
	SELECT DISTINCT ls.league_name, ls.league_country, r.season,
		CASE WHEN r.match_result = 0 THEN 'draw' WHEN r.match_result = 1 THEN 'home_win' ELSE 'away_win' END AS match_result
		,ROUND((COUNT(*) OVER (PARTITION BY r.league_id, r.season, r.match_result) / COUNT(*) OVER (PARTITION BY r.league_id, r.season)::numeric), 2) AS results_pcts
	FROM fact_results_test AS r
	JOIN (SELECT DISTINCT league_id, league_name, league_country FROM dim_league_season) AS ls ON ls.league_id = r.league_id
	ORDER BY  ls.league_name, r.season, match_result DESC
	);

-- match results pcts - group by, virtual table with correlated subquery
CREATE OR REPLACE VIEW v_match_results_pcts_league_season_v2 AS (
		SELECT ls.league_name, ls.league_country, t1.season
				,CASE WHEN t1.match_result = 0 THEN 'draw' WHEN t1.match_result = 1 THEN 'home_win' ELSE 'away_win' END AS match_result
				,t1.results_pcts
		FROM (SELECT r.league_id, r.season, r.match_result
					,round(COUNT(*) / (SELECT count(*) FROM fact_results_test AS r2 WHERE r2.league_id = r.league_id AND r2.season = r.season GROUP BY r2.league_id, r2.season)::numeric,2) AS results_pcts
				FROM fact_results_test AS r
				GROUP BY r.league_id, r.season,  r.match_result) AS t1
		JOIN (SELECT DISTINCT league_id, league_name, league_country FROM dim_league_season) AS ls ON ls.league_id = t1.league_id
		ORDER BY  ls.league_name, t1.season, t1.match_result DESC);


-- match results pcts - group by, self join virtual table,

CREATE OR REPLACE VIEW v_match_results_pcts_league_season_v3 AS (
			SELECT ls.league_name, ls.league_country, t1.season, t1.match_result, t1.results_pcts
			FROM
						(SELECT r.league_id, r.season
								,CASE WHEN r.match_result = 0 THEN 'draw' WHEN r.match_result = 1 THEN 'home_win' ELSE 'away_win' END AS match_result
								,round (COUNT(*) / r2.match_num_sum::numeric, 2) as results_pcts
						FROM fact_results_test AS r
						JOIN (SELECT r2.league_id, r2.season, COUNT(*) AS match_num_sum FROM fact_results_test AS r2 GROUP BY r2.league_id, r2.season) AS r2 ON r2.league_id = r.league_id AND r2.season = r.season
						GROUP BY r.league_id, r.season,  r.match_result, r2.match_num_sum) AS t1
			JOIN (SELECT DISTINCT league_id, league_name, league_country FROM dim_league_season) AS ls ON ls.league_id = t1.league_id
			ORDER BY  ls.league_name, t1.season, t1.match_result DESC);


-- 1. Test - no index
-- fact_results_test (3 784 704 rows)
-- queries duration test (no indexes)

-- avg duration: 8 s 112 msec
SELECT * FROM v_match_results_pcts_league_season;
-- avg duration: 4 sec 676 msec
SELECT * FROM v_match_results_pcts_league_season_v2;
-- avg duration: 1 sec 745 msec
SELECT * FROM v_match_results_pcts_league_season_v3;

EXPLAIN ANALYZE
SELECT * FROM v_match_results_pcts_league_season_v3;


-- 2. Test - index (league_id, season)
-- fact_results_test (3 784 704 rows)
-- queries duration test: index (league_id, season)

-- CREATE INDEX fact_results_league_season_idx ON fact_results_test (league_id, season)
-- DROP INDEX fact_results_league_season_idx

-- avg duration: 8 s 252 msec - no noticeable impact
SELECT * FROM v_match_results_pcts_league_season;
-- avg duration: 2 sec 247 msec - noticable impact (using clause 'WHERE' in correlated subquery)
SELECT * FROM v_match_results_pcts_league_season_v2;
-- avg duration: 1 sec 775 msec - no noticable impact
SELECT * FROM v_match_results_pcts_league_season_v3;