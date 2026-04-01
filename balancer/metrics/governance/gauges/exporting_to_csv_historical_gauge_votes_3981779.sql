-- part of a query repo
-- query name: exporting to csv - historical gauge votes
-- query link: https://dune.com/queries/3981779


WITH parsed_data AS (
    SELECT CAST(address as VARCHAR) as gauge, name as label
    FROM labels.balancer_gauges
), -- Gauge Labels

	labels AS (
	    SELECT gauge, label AS symbol
	    FROM parsed_data
	),

	vote_results AS (
		SELECT
		    provider AS wallet_address,
			round_id,
			start_date,
			end_date,
			v.gauge,
			symbol,
			vote
		FROM
			--vebal_votes
			balancer_ethereum.vebal_votes AS v
		LEFT JOIN labels l ON l.gauge = CAST(v.gauge AS VARCHAR)
	)
	
SELECT
	v.round_id,
	DATE_FORMAT(v.start_date, '%Y-%m-%d') AS start_date,
	DATE_FORMAT(v.end_date, '%Y-%m-%d') AS end_date,
	wallet_address,
	COALESCE(
		symbol,
		CONCAT(
			'0x',
			LOWER(SUBSTRING(to_hex(gauge), 1, 4)),
			'...',
			LOWER(SUBSTRING(to_hex(gauge), 37, 4))
		)
	) AS symbol,
	gauge,
	vote
FROM vote_results AS v
WHERE round_id < 123
