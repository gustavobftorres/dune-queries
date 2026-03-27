-- part of a query repo
-- query name: reCLAMM Liquidity Depth (USD): COW/WETH - Base
-- query link: https://dune.com/queries/5840419


SELECT 
    "minute",
    liquidity_depth_a as "Liquidity Depth"
FROM "query_5830780(chain='base', pool='0xff028c1ec4559d3aa2b0859aa582925b5cc28069',token_a='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', chain_a='ethereum')"