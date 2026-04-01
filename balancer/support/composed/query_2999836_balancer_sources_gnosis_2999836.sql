-- part of a query repo
-- query name: (query_2999836) balancer_sources_gnosis
-- query link: https://dune.com/queries/2999836


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
WHERE t1.blockchain = 'gnosis'
AND t1.tx_hash NOT IN (SELECT DISTINCT tx_hash FROM dex_aggregator.trades WHERE blockchain = 'gnosis')
),

routers as (
SELECT * FROM (values
(0x9008d19f58aabd9ed0d60971565aa8510560ab41, 'CoWSwap'),
(0x51a765f879d6dde820382f851957a1ab3a17ebbd, 'Gnosis Safe'),
(0x7f870d1c0e4c7fb4311b2855ed895ef06df26e74, 'Gnosis Safe'),
(0x3b33e333b2cb692bc98ac655ba92a592cd42d755, 'Gnosis Safe'),
(0x91eee3b0d3ed859cef69b06d7a66e690f3619129, 'Gnosis Safe'),
(0x0fd97eb416e8249962216ca56822a510eef30455, 'Gnosis Safe'),
(0xe0ce7200414865dc9f71838317b976abc82ab752, 'Gnosis Safe'),
(0xc14b8a37f4890ee46e0c6fa5b43d5913b0c062bf, 'Gnosis Safe'),
(0x741a58149a47563213e8b670077cf1b49e04289d, 'Gnosis Safe'),
(0x9fdb1eeeb2de440f2c90577f08c282a501e40c45, 'Gnosis Safe'),
(0xa50e73bdc44f8de5f909facd6562992f134bbed2, 'Gnosis Safe'),
(0x466015a8031ab19e759caa1a0c93d6ce92bad998, 'Gnosis Safe'),
(0x60be42f0044a38c6d121027384dd26b48d32781b, 'Gnosis Safe'),
(0x59a6f84aa07c13f9ef48220bbefaa14b42755a1d, 'Gnosis Safe'),
(0xf2d80d837f339a4693115764ee15db346b477682, 'Gnosis Safe'),
(0x6211334f56379706fb0f84e6eb952b52182e17df, 'Gnosis Safe'),
(0xfca7da0a0290d7bcbecd93be124756fc9b6f8e6a, 'Gnosis Safe'),
(0x111111125421ca6dc452d289314280a0f8842a65, '1inch'),
(0x1111111254eeb25477b68fb85ed929f73a960582, '1inch'),
(0xba12222222228d8ba445958a75a0704d566bf2c8, 'Vault'),
(0x8f41c31acc5e3ecf2fbbe6e36f3d28cce99ffbfe, 'XSwap'),
(0x6A000F20005980200259B80c5102003040001068, 'Paraswap'),
(0xe2fa4e1d17725e72dcdAfe943Ecf45dF4B9E285b, 'Direct Router'),
(0x86e67E115f96DF37239E0479441303De0de7bc2b, 'Direct Router'),
(0xC1A64500E035D9159C8826E982dFb802003227f0, 'Direct Router'),
(0x84813aA3e079A665C0B80F944427eE83cBA63617, 'Direct Router'))
    as t (address, name))
    
SELECT al.* FROM arbitrage_labels al
LEFT JOIN routers r ON al.address = r.address
WHERE r.address IS NULL
UNION ALL
SELECT address, name, 'gnosis' as blockchain FROM routers
