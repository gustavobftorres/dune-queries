-- part of a query repo
-- query name: reCLAMM Liquidity Depth (USD): EURC/USDC - Base
-- query link: https://dune.com/queries/5840495


SELECT 
    *,
    liquidity_depth_a as "Liquidity Depth"
FROM "query_5830780(chain='base', pool='0x12c2de9522f377b86828f6af01f58c046f814d3c',token_a='0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c',chain_a='ethereum')"