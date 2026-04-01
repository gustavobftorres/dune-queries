-- part of a query repo
-- query name: reCLAMM Price Comparison - USDC.e/GNO Gnosis
-- query link: https://dune.com/queries/5840112


SELECT
    "minute",
    "Market",
    "reCLAMM"
-- pool_balancer = COW/WETH on Base
-- pool_aero = COW/WETH on Base
-- token_a = WETH on Ethereum
-- token_b = COW on Ethereum
FROM "query_5757256(pool_balancer='0x70b3b56773ace43fe86ee1d80cbe03176cbe4c09',pool_aero='0x000000000000000000000000000000000000000000',token_a='0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',token_b='0x6810e776880c02933d47db1b9fc05908e5386b96')"
WHERE "minute" >= date_trunc('day', now() - interval '2' day)