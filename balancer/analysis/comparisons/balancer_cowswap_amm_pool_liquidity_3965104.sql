-- part of a query repo
-- query name: Balancer CoWSwap AMM Pool Liquidity
-- query link: https://dune.com/queries/3965104


SELECT 
    CAST(day AS TIMESTAMP) AS day, 
    pool_symbol, 
    SUM(protocol_liquidity_usd) AS tvl_usd, 
    SUM(protocol_liquidity_eth) AS tvl_eth
FROM balancer_cowswap_amm.liquidity
WHERE day <= TIMESTAMP '{{3. End date}}'
AND day >= TIMESTAMP '{{2. Start date}}'
AND pool_id = {{1. Pool Address}}
AND blockchain = '{{4. Blockchain}}'
GROUP BY 1, 2
