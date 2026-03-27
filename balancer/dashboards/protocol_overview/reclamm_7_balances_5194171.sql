-- part of a query repo
-- query name: ReCLAMM #7 Balances
-- query link: https://dune.com/queries/5194171


SELECT day, token_symbol, pool_liquidity_usd
FROM balancer.liquidity
WHERE blockchain = 'base'
AND pool_address = 0xc46e6A1CB1910c916620Dc81C7fd8c38891E1904
AND version = '3'
ORDER BY 1, 2
