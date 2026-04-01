-- part of a query repo
-- query name: Balancer CoWSwap AMM Token Balances
-- query link: https://dune.com/queries/3965112


SELECT 
    CAST(day as timestamp) as day,
    COALESCE(b.token_symbol, SUBSTRING(CAST(token_address as VARCHAR), 0, 6)) AS symbol,
    protocol_liquidity_usd AS token_balance
FROM balancer_cowswap_amm.liquidity b
WHERE pool_address = {{1. Pool Address}}
        AND blockchain = '{{4. Blockchain}}'