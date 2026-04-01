-- part of a query repo
-- query name: veBAL Votes by Gauge
-- query link: https://dune.com/queries/2638055


WITH
	parsed_data AS (SELECT * FROM query_2330097), -- Guage Labels

	labels AS (SELECT gauge, label AS symbol FROM parsed_data),
	
	vote_results AS (
		SELECT
			end_date,
			gauge,
			SUM(vote) AS votes
		FROM
			--vebal_votes
			query_2265987 AS v
		GROUP BY 1, 2
		ORDER BY 1, 2
	),
	total_votes AS (
		SELECT
			end_date,
			SUM(votes) AS total_votes
		FROM vote_results
		GROUP BY 1
	),
	top_gauges AS (
		SELECT
		v.gauge,
		COALESCE(symbol,
			CONCAT(
				'0x',
				LOWER(SUBSTRING(to_hex(v.gauge), 1, 4)),
				'...',
				LOWER(SUBSTRING(to_hex(v.gauge), 37, 4))
			)
		) AS symbol
		FROM vote_results v
		LEFT JOIN labels l ON l.gauge = CAST(v.gauge AS VARCHAR)	
		ORDER BY end_date DESC, votes DESC
		LIMIT 15
	)
	
SELECT
	v.end_date,
	COALESCE(g.symbol, 'Others') AS symbol,
	SUM(v.votes) / total_votes AS pct_votes,
	SUM(v.votes) AS votes
FROM vote_results AS v
LEFT JOIN total_votes AS t ON t.end_date = v.end_date
LEFT JOIN top_gauges AS g ON g.gauge = v.gauge
GROUP BY 1, 2, total_votes
ORDER BY 1 DESC, 3 DESC