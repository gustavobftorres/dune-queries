-- part of a query repo
-- query name: ReCLAMM #4 Balances
-- query link: https://dune.com/queries/5194152


SELECT day, token_symbol, pool_liquidity_usd
FROM balancer.liquidity
WHERE blockchain = 'base'
AND pool_address = 0x785D9232cB7195A7ddBA3864f30B750FD7596faC
AND version = '3'
ORDER BY 1, 2
