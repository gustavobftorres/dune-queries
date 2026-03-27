-- part of a query repo
-- query name: Token Balances
-- query link: https://dune.com/queries/3108117


SELECT 
    CAST(day as timestamp) as day,
    COALESCE(b.token_symbol, SUBSTRING(CAST(token_address as VARCHAR), 0, 6)) AS symbol,
    pool_liquidity_usd as usd_amount
FROM balancer.liquidity b
WHERE CAST(pool_id as VARCHAR) = '{{1. Pool ID}}'
        AND blockchain = '{{4. Blockchain}}'