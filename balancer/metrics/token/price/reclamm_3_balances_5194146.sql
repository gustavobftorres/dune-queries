-- part of a query repo
-- query name: ReCLAMM #3 Balances
-- query link: https://dune.com/queries/5194146


SELECT day, token_symbol, pool_liquidity_usd
FROM balancer.liquidity
WHERE blockchain = 'base'
AND pool_address = 0x6B54B954E53c3fBaf84B6b97377f3760C91DB847
AND version = '3'
ORDER BY 1, 2
