-- part of a query repo
-- query name: BPT Prices
-- query link: https://dune.com/queries/3360296


WITH tvl AS(
SELECT 
    day,
    blockchain,
    pool_address,
    sum(protocol_liquidity_usd) AS liquidity
FROM balancer.liquidity
GROUP BY 1,2,3)

SELECT
    t.day,
    t.blockchain,
    t.pool_address,
    liquidity / supply AS price
FROM tvl t
LEFT JOIN test_schema.git_dunesql_c6a21c9e_balancer_bpt_supply s ON t.day = s.day
AND t.pool_address = s.token_address AND t.blockchain = s.blockchain
WHERE supply > 0