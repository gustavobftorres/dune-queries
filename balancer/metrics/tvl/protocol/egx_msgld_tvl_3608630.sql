-- part of a query repo
-- query name: egx/msgld tvl
-- query link: https://dune.com/queries/3608630


SELECT CAST(day AS TIMESTAMP) AS day, sum(protocol_liquidity_usd) AS tvl
FROM balancer_v2_polygon.liquidity
WHERE pool_id = 0xf578d7ed134c52e407363bb0c06b369e212d32700002000000000000000003f5
GROUP BY 1