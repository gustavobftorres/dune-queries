-- part of a query repo
-- query name: ReCLAMM #6 Balances
-- query link: https://dune.com/queries/5194163


SELECT day, token_symbol, pool_liquidity_usd
FROM balancer.liquidity
WHERE blockchain = 'base'
AND pool_address = 0x7Dc81fb7e93cdde7754bff7f55428226bD9cEF7b
AND version = '3'
ORDER BY 1, 2
