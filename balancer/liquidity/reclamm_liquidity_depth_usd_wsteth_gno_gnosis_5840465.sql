-- part of a query repo
-- query name: reCLAMM Liquidity Depth (USD): wstETH/GNO - Gnosis
-- query link: https://dune.com/queries/5840465


SELECT 
    *,
    liquidity_depth_a as "Liquidity Depth"
FROM "query_5830780(chain='gnosis',pool='0xa50085ff1dfa173378e7d26a76117d68d5eba539',start='2025-09-17 12:00:00',token_a='0x6810e776880c02933d47db1b9fc05908e5386b96',chain_a='ethereum')"