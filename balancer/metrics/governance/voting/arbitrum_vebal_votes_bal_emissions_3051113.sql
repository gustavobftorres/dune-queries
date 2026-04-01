-- part of a query repo
-- query name: Arbitrum veBAL votes & BAL emissions
-- query link: https://dune.com/queries/3051113


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
			'polygon' AS gauge_type
		FROM
			balancer_ethereum.CappedPolygonRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		SELECT
			gauge,
			'arbitrum' AS gauge_type
		FROM
			balancer_ethereum.ArbitrumRootGaugeFactory_evt_ArbitrumRootGaugeCreated
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
			balancer_ethereum.OptimismRootGaugeFactory_evt_OptimismRootGaugeCreated
		UNION ALL
		SELECT
			gauge,
			'optimism' AS gauge_type
		FROM
			balancer_ethereum.CappedOptimismRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		SELECT
			gauge,
			'avalanche_c' AS gauge_type
		FROM
			balancer_ethereum.AvalancheRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		SELECT
			gauge,
			'base' AS gauge_type
		FROM
		    balancer_ethereum.BaseRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		SELECT
			gauge,
			'gnosis' AS gauge_type
		FROM
		    balancer_ethereum.GnosisRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		SELECT
			gauge,
			'polygon zkEVM' AS gauge_type
		FROM
		    balancer_ethereum.PolygonZkEVMRootGaugeFactory_evt_GaugeCreated
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
	
, final AS (
    SELECT
    	v.end_date,
    	COALESCE(v.gauge_type, 'ethereum') AS gauge_type,
    	SUM(v.votes) / total_votes AS pct_votes,
    	SUM(v.votes) AS votes
    FROM
    	vote_results AS v
    LEFT JOIN total_votes AS t ON t.end_date = v.end_date
    GROUP BY 1, 2, total_votes
)

SELECT --*
    end_date
    , gauge_type
    , pct_votes
    , votes
    , x.pct_votes * y.week_rate AS bal_emissions
    , x.pct_votes * y.week_rate * z.median_price AS bal_emissions_usd
    , z.median_price
FROM final x
INNER JOIN query_2511140 y
    ON y.time = x.end_date
INNER JOIN (
    SELECT date_trunc('day', minute) AS day, approx_percentile(price, 0.5) AS median_price
    FROM prices.usd WHERE contract_address = 0xba100000625a3754423978a60c9317c58a424e3d
    GROUP BY 1
) z
    ON z.day = x.end_date
WHERE gauge_type = 'arbitrum'
ORDER BY 1 DESC, 3 DESC