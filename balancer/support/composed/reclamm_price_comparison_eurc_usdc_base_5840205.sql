-- part of a query repo
-- query name: reCLAMM Price Comparison - EURC/USDC Base
-- query link: https://dune.com/queries/5840205


SELECT
    "minute",
    "Market",
    "reCLAMM",
    "reCLAMM (Trades)"
-- pool_balancer = COW/WETH on Base
-- pool_aero = COW/WETH on Base
-- token_a = WETH on Ethereum
-- token_b = COW on Ethereum
FROM "query_5757256(pool_balancer='0x12c2de9522f377b86828f6af01f58c046f814d3c',pool_aero='0x000000000000000000000000000000000000000000',token_a='0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c',token_b='0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')"
WHERE "minute" >= date_trunc('day', now() - interval '2' day)