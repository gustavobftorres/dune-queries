-- part of a query repo
-- query name: reCLAMM Liquidity Depth (USD): USDC.e/GNO - Gnosis
-- query link: https://dune.com/queries/5840474


SELECT 
    *,
    liquidity_depth_a as "Liquidity Depth"
FROM "query_5830780(chain='gnosis',pool='0x70b3b56773ace43fe86ee1d80cbe03176cbe4c09',start='2025-09-17 10:15:00',token_a='0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',chain_a='ethereum')"