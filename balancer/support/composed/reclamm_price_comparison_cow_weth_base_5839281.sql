-- part of a query repo
-- query name: reCLAMM Price Comparison - COW/WETH Base
-- query link: https://dune.com/queries/5839281


SELECT
    "minute",
    "Market",
    "Aero (Base)",
    "reCLAMM",
    "reCLAMM (Trades)"
-- pool_balancer = COW/WETH on Base
-- pool_aero = COW/WETH on Base
-- token_a = WETH on Ethereum
-- token_b = COW on Ethereum
FROM "query_5757256(pool_balancer='0xff028c1ec4559d3aa2b0859aa582925b5cc28069',pool_aero='0x155e0971A2392c446be02373A4F4c8dC4266f015',token_a='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',token_b='0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab')"
WHERE "minute" >= date_trunc('day', now() - interval '2' day)