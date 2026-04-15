-- part of a query repo
-- query name: pmUSD / USDC Volume
-- query link: https://dune.com/queries/6927109


SELECT 
    SUM(CASE WHEN block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '1' DAY 
        THEN amount_usd END)/1e6 AS volume_24h
FROM balancer.trades 
WHERE project_contract_address = 0xe00e947decfe01692070e113002705bdf77ddbd3
AND blockchain = 'ethereum'
