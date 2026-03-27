-- part of a query repo
-- query name: jitoSOL/wSOL TVL
-- query link: https://dune.com/queries/3751238


SELECT day, sum(protocol_liquidity_usd) AS tvl FROM balancer_v2_arbitrum.liquidity
WHERE pool_id = 0xfb2f7ed572589940e24c5711c002adc59d5e79ef000000000000000000000535
GROUP BY 1