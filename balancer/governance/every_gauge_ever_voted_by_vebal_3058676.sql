-- part of a query repo
-- query name: every gauge ever voted by veBAL
-- query link: https://dune.com/queries/3058676


WITH 
    gauges AS (
        -- First appearance of every gauge
        SELECT * FROM (
            SELECT 
                round_id
                , start_date
                , end_date
                , gauge
                , ROW_NUMBER() OVER(PARTITION BY gauge ORDER BY round_id ASC) AS rn
            FROM query_2265987
        ) WHERE rn = 1
    )
    , root_gauges_created AS (
        SELECT
			gauge
			, contract_address
			, 'arbitrum' AS gauge_type
		FROM
			balancer_ethereum.ArbitrumRootGaugeFactory_evt_ArbitrumRootGaugeCreated
		UNION ALL
		SELECT
			gauge
			, contract_address
			, 'arbitrum capped' AS gauge_type
		FROM
			balancer_ethereum.CappedArbitrumRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		
		SELECT
			gauge
			, contract_address
			, 'optimism' AS gauge_type
		FROM
			balancer_ethereum.OptimismRootGaugeFactory_evt_OptimismRootGaugeCreated
		UNION ALL
		SELECT
			gauge
			, contract_address
			, 'optimism capped' AS gauge_type
		FROM
			balancer_ethereum.CappedOptimismRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		
        SELECT
			gauge
			, contract_address
			, 'polygon' AS gauge_type
		FROM
			balancer_ethereum.PolygonRootGaugeFactory_evt_PolygonRootGaugeCreated
		UNION ALL
		SELECT
			gauge
			, contract_address
			, 'polygon capped' AS gauge_type
		FROM
			balancer_ethereum.CappedPolygonRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		
		SELECT
			gauge
			, contract_address
			, 'avalanche_c' AS gauge_type
		FROM
			balancer_ethereum.AvalancheRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		SELECT
			gauge
			, contract_address
			, 'base' AS gauge_type
		FROM
		    balancer_ethereum.BaseRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		SELECT
			gauge
			, contract_address
			, 'gnosis' AS gauge_type
		FROM
		    balancer_ethereum.GnosisRootGaugeFactory_evt_GaugeCreated
		UNION ALL
		SELECT
			gauge
			, contract_address
			, 'polygon zkEVM' AS gauge_type
		FROM
		    balancer_ethereum.PolygonZkEVMRootGaugeFactory_evt_GaugeCreated
		---------
		UNION ALL
		SELECT 
    		gauge
			, contract_address
			, 'mainnet capped liquidity' AS gauge_type 
		FROM balancer_ethereum.CappedLiquidityGaugeFactory_evt_GaugeCreated
		UNION ALL
		SELECT 
    		gauge
			, contract_address
			, 'mainnet liquidity' AS gauge_type 
		FROM balancer_ethereum.LiquidityGaugeFactory_evt_GaugeCreated
		UNION ALL
		SELECT 
    		gauge
			, contract_address
			, 'mainnet single recipient vX' AS gauge_type 
		FROM balancer_ethereum.SingleRecipientGaugeFactory_evt_SingleRecipientGaugeCreated
		UNION ALL
		SELECT 
    		gauge
			, contract_address
			, 'mainnet single recipient vY' AS gauge_type 
		FROM balancer_ethereum.SingleRecipientGaugeFactory_evt_GaugeCreated
    )
SELECT round_id, x.gauge, y.gauge = x.gauge, y.gauge_type
FROM gauges x
LEFT JOIN root_gauges_created y ON y.gauge = x.gauge
WHERE gauge_type NOT LIKE '%mainnet%' --164