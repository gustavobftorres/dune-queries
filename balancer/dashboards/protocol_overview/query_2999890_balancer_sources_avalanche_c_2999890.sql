-- part of a query repo
-- query name: (query_2999890) balancer_sources_avalanche_c
-- query link: https://dune.com/queries/2999890


WITH arbitrage_labels as
(
SELECT
    DISTINCT(t1.tx_to) as address,
    'Arbitrage Bot' as name,
    t1.blockchain
FROM dex.trades t1
INNER JOIN dex.trades t2
ON t1.tx_hash = t2.tx_hash AND t1.token_bought_address = t2.token_sold_address
AND t1.token_sold_address = t2.token_bought_address
AND t1.blockchain = t2.blockchain
AND (t1.project = 'balancer' AND t2.project != 'balancer' 
OR t2.project = 'balancer' AND t1.project != 'balancer')
WHERE t1.blockchain = 'avalanche_c'
AND t1.tx_hash NOT IN (SELECT DISTINCT tx_hash FROM dex_aggregator.trades WHERE blockchain = 'avalanche_c')
),

routers as (
SELECT * FROM (values
(0x1111111254eeb25477b68fb85ed929f73a960582, '1Inch'),
(0x056c41b8c2a2e7c6454842c9a62050fa1b5ffbae, 'Cian'),
(0x1f076a800005c758a505e759720eb6737136e893, 'DODO'),
(0x7c5c4af1618220c090a6863175de47afb20fa9df, 'Gelato'),
(0x7b2e3fc7510d1a51b3bef735f985446589219354, 'Interport'),
(0xcd2e3622d483c7dc855f72e5eafadcd577ac78b4, 'LayerZero'),
(0x6a000f20005980200259b80c5102003040001068, 'Paraswap'),
(0x88de50b233052e4fb783d4f6db78cc34fea3e9fc, 'ODOS'),
(0xe1e7065235d59a71da419d5004015eb3a2b3d5b0, 'TraderJoe'),
(0x1d7a1a79e2b4ef88d2323f3845246d24a3c20f1d, 'TraderJoe'),
(0xba12222222228d8ba445958a75a0704d566bf2c8, 'Vault')
)
    as t (address, name))
    
SELECT al.* FROM arbitrage_labels al
LEFT JOIN routers r ON al.address = r.address
WHERE r.address IS NULL
UNION ALL
SELECT address, name, 'avalanche_c' as blockchain FROM routers
