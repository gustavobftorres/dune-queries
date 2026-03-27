-- part of a query repo
-- query name: veBAL votes by network
-- query link: https://dune.com/queries/2638061


WITH
	non_eth_mainnet_gauges AS (
		SELECT
			0xe867ad0a48e8f815dc0cda2cdb275e0f163a480b AS gauge,
			'veBAL' AS gauge_type
		UNION ALL
		SELECT
			gauge,
			'polygon' AS gauge_type
		FROM
			balancer_ethereum.PolygonRootGaugeFactory_evt_PolygonRootGaugeCreated
		UNION ALL
		SELECT
			gauge,
			'arbitrum' AS gauge_type
		FROM
			balancer_ethereum.ArbitrumRootGaugeFactory_evt_ArbitrumRootGaugeCreated
		UNION ALL
		SELECT
			gauge,
			'polygon' AS gauge_type
		FROM
			balancer_ethereum.CappedPolygonRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		SELECT
			gauge,
			'arbitrum' AS gauge_type
		FROM
			balancer_ethereum.CappedArbitrumRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		SELECT
			gauge,
			'optimism' AS gauge_type
		FROM
			balancer_ethereum.CappedOptimismRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		SELECT
			gauge,
			'optimism' AS gauge_type
		FROM
			balancer_ethereum.OptimismRootGaugeFactory_evt_OptimismRootGaugeCreated
	),
	vote_results AS (
		SELECT
			end_date,
			gauge_type,
			SUM(vote) AS votes
		FROM
			--vebal_votes
			query_2265987 AS v
		LEFT JOIN non_eth_mainnet_gauges AS l ON l.gauge = v.gauge
		GROUP BY 1, 2
	),
	total_votes AS (
		SELECT end_date, SUM(votes) AS total_votes
		FROM vote_results
		GROUP BY 1
	)
	
SELECT
	v.end_date,
	COALESCE(v.gauge_type, 'ethereum') AS gauge_type,
	SUM(v.votes) / total_votes AS pct_votes,
	SUM(v.votes) AS votes
FROM
	vote_results AS v
LEFT JOIN total_votes AS t ON t.end_date = v.end_date
GROUP BY 1, 2, total_votes
ORDER BY 1 DESC, 3 DESC
