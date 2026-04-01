-- part of a query repo
-- query name: ReCLAMM #2 Balances
-- query link: https://dune.com/queries/5194142


SELECT day, token_symbol, pool_liquidity_usd
FROM balancer.liquidity
WHERE blockchain = 'base'
AND pool_address = 0xd9a8bd46fbB0BaC27aA1A99E64931d406e3bBb3F
AND version = '3'
ORDER BY 1, 2
