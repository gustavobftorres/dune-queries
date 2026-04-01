-- part of a query repo
-- query name: EGX pools - TVL
-- query link: https://dune.com/queries/3518104


SELECT day, pool_symbol,  SUM(protocol_liquidity_usd) AS tvl FROM balancer_v2_polygon.liquidity
WHERE pool_id IN (
0xf578d7ed134c52e407363bb0c06b369e212d32700002000000000000000003f5,
0x241df159b03a90455edee61625655fc0ea5fa3dd0002000000000000000003fc)
GROUP BY 1, 2