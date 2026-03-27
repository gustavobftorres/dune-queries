-- part of a query repo
-- query name: Pool Liquidity
-- query link: https://dune.com/queries/2638028


SELECT 
    CAST(day AS TIMESTAMP) AS day, 
    pool_symbol, 
    SUM(pool_liquidity_usd) AS tvl_usd, 
    SUM(pool_liquidity_eth) AS tvl_eth
FROM balancer.liquidity
WHERE day <= TIMESTAMP '{{3. End date}}'
AND day >= TIMESTAMP '{{2. Start date}}'
AND pool_id = {{1. Pool ID}}
AND blockchain = '{{4. Blockchain}}'
GROUP BY 1, 2
ORDER BY 1 DESC