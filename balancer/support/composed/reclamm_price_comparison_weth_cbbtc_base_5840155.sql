-- part of a query repo
-- query name: reCLAMM Price Comparison - WETH/cbBTC Base
-- query link: https://dune.com/queries/5840155


SELECT
    "minute",
    "Market",
    "reCLAMM",
    "reCLAMM (Trades)"
-- pool_balancer = COW/WETH on Base
-- pool_aero = COW/WETH on Base
-- token_a = WETH on Ethereum
-- token_b = COW on Ethereum
FROM "query_5757256(pool_balancer='0x19aeb8168d921bb069c6771bbaff7c09116720d0',pool_aero='0x000000000000000000000000000000000000000000',token_a='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',token_b='0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf')"
WHERE "minute" >= date_trunc('day', now() - interval '2' day)