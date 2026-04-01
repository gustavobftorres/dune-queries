-- part of a query repo
-- query name: Balancer Average Swap Fee on Polygon
-- query link: https://dune.com/queries/98619


WITH fees AS (
        SELECT LAST_VALUE("swapFeePercentage"/1e16) OVER (PARTITION BY contract_address ORDER BY evt_block_time) AS fee
        FROM balancer_v2."WeightedPool_evt_SwapFeePercentageChanged"
        
        UNION ALL
        
        SELECT LAST_VALUE("swapFeePercentage"/1e16) OVER (PARTITION BY contract_address ORDER BY evt_block_time) AS fee
        FROM balancer_v2."StablePool_evt_SwapFeePercentageChanged"
    )

SELECT AVG(fee) AS avgfee FROM fees