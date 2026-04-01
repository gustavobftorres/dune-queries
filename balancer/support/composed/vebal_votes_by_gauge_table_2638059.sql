-- part of a query repo
-- query name: veBAL Votes by Gauge Table
-- query link: https://dune.com/queries/2638059


WITH

	parsed_data AS (SELECT * FROM query_2330097), -- Guage Labels

	labels AS (SELECT gauge, label AS symbol FROM parsed_data),

	vote_results AS (
		SELECT
			round_id,
			start_date,
			end_date,
			v.gauge,
			symbol,
			SUM(vote) AS votes
		FROM
			--vebal_votes
			query_2265987 AS v
		LEFT JOIN labels l ON l.gauge = CAST(v.gauge AS VARCHAR)
		GROUP BY 1, 2, 3, 4, 5
	),
	total_votes AS (
		SELECT round_id, SUM(votes) AS total_votes
		FROM vote_results
		GROUP BY 1
	)
	
SELECT
	v.round_id,
	DATE_FORMAT(v.start_date, '%Y-%m-%d') AS start_date,
	DATE_FORMAT(v.end_date, '%Y-%m-%d') AS end_date,
	--SUBSTRING(v.start_date, 0, 11) AS start_date,
	--SUBSTRING(v.end_date, 0, 11) AS end_date,
	COALESCE(
		symbol,
		CONCAT(
			'0x',
			LOWER(SUBSTRING(to_hex(gauge), 1, 4)),
			'...',
			LOWER(SUBSTRING(to_hex(gauge), 37, 4))
		)
	) AS symbol,
	votes,
	votes / total_votes AS pct_votes
FROM vote_results AS v
JOIN total_votes AS t ON t.round_id = v.round_id
ORDER BY 1 DESC, 5 DESC
