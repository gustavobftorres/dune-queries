-- part of a query repo
-- query name: ve8020 Pool Balances
-- query link: https://dune.com/queries/3107818


SELECT 
    CAST(day as timestamp) as day,
    COALESCE(b.token_symbol, SUBSTRING(CAST(token_address as VARCHAR), 1, 6)) AS symbol,
    pool_liquidity_usd as usd_amount
FROM balancer.liquidity b
WHERE BYTEARRAY_SUBSTRING(pool_id,1,20) = {{Pool Address}}
AND blockchain = '{{Blockchain}}'
