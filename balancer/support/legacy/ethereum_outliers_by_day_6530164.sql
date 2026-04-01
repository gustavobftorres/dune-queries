-- part of a query repo
-- query name: Ethereum outliers by day
-- query link: https://dune.com/queries/6530164


SELECT
    day,
    token_symbol,
    SUM(protocol_liquidity_usd) AS tvl
FROM test_schema.git_dunesql_69b226d_balancer_v2_ethereum_liquidity
GROUP BY 1, 2
HAVING SUM(protocol_liquidity_usd) > 1000000000
ORDER BY day ASC;