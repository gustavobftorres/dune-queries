-- part of a query repo
-- query name: reCLAMM Liquidity Depth (USD): WETH/AAVE - Arbitrum
-- query link: https://dune.com/queries/5840484


SELECT 
    *, liquidity_depth_a as "Liquidity Depth"
FROM "query_5830780(chain='arbitrum', pool='0x5ea58d57952b028c40bd200e5aff20fc4b590f51',token_a='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',chain_a='ethereum')"