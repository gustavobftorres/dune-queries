-- part of a query repo
-- query name: ReCLAMM #5 Balances
-- query link: https://dune.com/queries/5194155


SELECT day, token_symbol, pool_liquidity_usd
FROM balancer.liquidity
WHERE blockchain = 'base'
AND pool_address = 0x63B52EBA7e565CcEC991910Bd3482D01bA3Bf70d
AND version = '3'
ORDER BY 1, 2
