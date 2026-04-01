-- part of a query repo
-- query name: (query_2998457) balancer_sources_polygon
-- query link: https://dune.com/queries/2998457


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
WHERE t1.blockchain = 'polygon'
AND t1.tx_hash NOT IN (SELECT DISTINCT tx_hash FROM dex_aggregator.trades WHERE blockchain = 'polygon')
),

routers as (
SELECT * FROM (values
(0xdef1c0ded9bec7f1a1670819833240f027b25eff, '0x'),
(0x11111112542d85b3ef69ae05771c2dccff4faa26, '1inch'),
(0x1111111254eeb25477b68fb85ed929f73a960582, '1inch'),
(0x1111111254fb6c44bac0bed2854e76f90643097d, '1inch'),
(0xad3b67bca8935cb510c8d18bd45f0b94f54a968f, '1inch'),
(0xaf5457ebcc6c2afc56f044d9cc2484ec2b34142a, 'Chainhop'),
(0x375f6b0cd12b34dc28e34c26853a37012c24dde5, 'Gnosis Safe'),
(0xbba61015763488c11c209998db06a3d12fc43340, 'Gnosis Safe'),
(0x20d61737f972eecb0af5f0a85ab358cd083dd56a, 'Gnosis Safe'),
(0x1a2ce410a034424b784d4b228f167a061b94cff4, 'Gnosis Safe'),
(0x826b8d2d523e7af40888754e3de64348c00b99f4, 'Gnosis Safe'),
(0xf1423fec735cdd79468dc1804373bf72fe30fd5f, 'Gnosis Safe'),
(0x08cdd2b9b782d1debb987d2c43179229b24f397e, 'Gnosis Safe'),
(0xd8233dec505d6fa4dc126f6e8c7da6a8360a0004, 'Gnosis Safe'),
(0x0c54a0bccf5079478a144dbae1afcb4fedf7b263, 'Gnosis Safe'),
(0xf731577e6306403f635907cc6a144531fc2192f3, 'Gnosis Safe'),
(0xe8c3d8ff9356b68f68d87e5c0f6d39afde178e6e, 'Gnosis Safe'),
(0x8c8e076cd7d2a17ba2a5e5af7036c2b2b7f790f6, 'Gnosis Safe'),
(0xcc16d636dd05b52ff1d8b9ce09b09bc62b11412b, 'Gnosis Safe'),
(0xd211a02a0adde56bb7f9700f49d4ba832adc7ddf, 'Gnosis Safe'),
(0x1a1ec25dc08e98e5e93f1104b5e5cdd298707d31, 'Metamask'),
(0x6b3712943a913eb9a22b71d4210de6158c519970, 'Overnight'),
(0x44fdf9e0c9e52123ab484c9235694cc166ce5718, 'Overnight'),
(0xdef171fe48cf0115b1d80b88dc8eab59176fee57, 'Paraswap'),
(0x6a000f20005980200259b80c5102003040001068, 'Paraswap'),
(0xF2e4209afA4C3c9eaA3Fb8e12eeD25D8f328171C, 'Slingshot'),
(0xba12222222228d8ba445958a75a0704d566bf2c8, 'Vault'),
(0xedeafdef0901ef74ee28c207be8424d3b353d97a, 'Odos')
)
    as t (address, name))
    
SELECT al.* FROM arbitrage_labels al
LEFT JOIN routers r ON al.address = r.address
WHERE r.address IS NULL
UNION ALL
SELECT address, name, 'polygon' as blockchain FROM routers
