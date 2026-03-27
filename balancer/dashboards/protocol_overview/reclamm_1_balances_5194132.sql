-- part of a query repo
-- query name: ReCLAMM #1 Balances
-- query link: https://dune.com/queries/5194132


SELECT day, token_symbol, pool_liquidity_usd
FROM balancer.liquidity
WHERE blockchain = 'base'
AND pool_address = 0x1A0cde11fD13E9E347088e4cDc00801997911A75
AND version = '3'
ORDER BY 1, 2
