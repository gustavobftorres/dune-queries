-- part of a query repo
-- query name: UNIV2 Strategy Bots
-- query link: https://dune.com/queries/4846273


SELECT
    t1.*
FROM dex.trades t1
INNER JOIN dex.trades t2
ON t1.tx_hash = t2.tx_hash AND t1.token_bought_address = t2.token_sold_address
AND t1.token_sold_address = t2.token_bought_address
AND t1.blockchain = t2.blockchain
AND (t1.project = 'balancer' AND t2.project != 'balancer' 
OR t2.project = 'balancer' AND t1.project != 'balancer')
WHERE t1.blockchain = 'base'
AND t1.tx_to IN (0xf6a1070fc1e76e9c6eba3478f4b1cfa6081b7a30, 0x9903fbd663b3fcdd86e0414d4abc841fb3534ce9)
AND t1.tx_hash NOT IN (SELECT DISTINCT tx_hash FROM dex_aggregator.trades WHERE blockchain = 'base')