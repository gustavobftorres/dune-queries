-- part of a query repo
-- query name: reCLAMM Price Comparison - WETH/AAVE Arbitrum
-- query link: https://dune.com/queries/5840185


SELECT
    "minute",
    "Market",
    "reCLAMM"
-- pool_balancer = COW/WETH on Base
-- pool_aero = COW/WETH on Base
-- token_a = WETH on Ethereum
-- token_b = COW on Ethereum
FROM "query_5757256(pool_balancer='0x5ea58d57952b028c40bd200e5aff20fc4b590f51',pool_aero='0x000000000000000000000000000000000000000000',token_a='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',token_b='0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9')"
WHERE "minute" >= date_trunc('day', now() - interval '2' day)