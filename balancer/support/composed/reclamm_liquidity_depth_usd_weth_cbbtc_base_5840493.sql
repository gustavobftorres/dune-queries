-- part of a query repo
-- query name: reCLAMM Liquidity Depth (USD): WETH/cbBTC - Base
-- query link: https://dune.com/queries/5840493


SELECT 
   *,
   liquidity_depth_a as "Liquidity Depth"
FROM "query_5830780(chain='base',pool='0x19aeb8168d921bb069c6771bbaff7c09116720d0',token_a='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',chain_a='ethereum')"