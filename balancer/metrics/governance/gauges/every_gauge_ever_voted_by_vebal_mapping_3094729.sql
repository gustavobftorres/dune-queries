-- part of a query repo
-- query name: every gauge ever voted by veBAL mapping
-- query link: https://dune.com/queries/3094729


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

, final AS (
    SELECT round_id, x.gauge, y.gauge = x.gauge, y.gauge_type
    FROM gauges x
    LEFT JOIN root_gauges_created y ON y.gauge = x.gauge
    --WHERE gauge_type NOT LIKE '%mainnet%' 
)
SELECT * FROM final f
LEFT JOIN (
    -- SELECT 'arbitrum' AS blockchain, * FROM query_
    -- UNION ALL
    SELECT 'arbitrum capped' AS blockchain, * FROM query_3094768 
    UNION ALL
    -- SELECT 'optimism' AS blockchain, * FROM query_
    -- UNION ALL
    SELECT 'optimism capped' AS blockchain, * FROM query_3094774 -- childchainguage listed under balancer_v2 needs to be listed under balancer namespace
    UNION ALL
    -- SELECT 'polygon' AS blockchain, * FROM query_
    -- UNION ALL
    SELECT 'polygon capped' AS blockchain, * FROM query_3094785 -- childchainguage & factory not listed under balancer namespace QUERY IS CURRENTLY using balancer_v2 CHANGE
    UNION ALL
    SELECT 'avalanche_c' AS blockchain, * FROM query_3094753 
    UNION ALL
    SELECT 'base' AS blockchain, * FROM query_3094743
    UNION ALL
    SELECT 'gnosis' AS blockchain, * FROM query_3094283
    -- UNION ALL
    -- SELECT 'polygon zkEVM' AS blockchain, * FROM query_3091308 -- blockchain not on dune yet
    UNION ALL
    SELECT 'mainnet capped liquidity' AS blockchain, root_gauge, NULL AS recipient, NULL AS child_chain_gauge, pool_address FROM query_3094883
    UNION ALL
    SELECT 'mainnet liquidity' AS blockchain, root_gauge, NULL AS recipient, NULL AS child_chain_gauge, pool_address FROM query_3094906
    -- Below only represent 9 Gaugess
    -- UNION ALL
    -- SELECT 'mainnet single recipient vX' AS blockchain, root_gauge, NULL AS recipient, NULL AS child_chain_gauge, pool_address FROM query_ balancer_ethereum.SingleRecipientGaugeFactory_evt_SingleRecipientGaugeCreated
    -- UNION ALL
    -- SELECT 'mainnet single recipient vY' AS blockchain, root_gauge, NULL AS recipient, NULL AS child_chain_gauge, pool_address FROM query_ balancer_ethereum.SingleRecipientGaugeFactory_evt_GaugeCreated
    
) x
ON x.root_gauge = f.gauge
AND x.blockchain = f.gauge_type
