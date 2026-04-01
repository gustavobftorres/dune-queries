-- part of a query repo
-- query name: (query_3033138) balancer_sources_base
-- query link: https://dune.com/queries/3033138


WITH arbitrage_labels as
(
SELECT DISTINCT
    (t1.tx_to) as address,
    'Arbitrage Bot' as name,
    t1.blockchain
FROM dex.trades t1
INNER JOIN dex.trades t2
ON t1.tx_hash = t2.tx_hash AND t1.token_bought_address = t2.token_sold_address
AND t1.token_sold_address = t2.token_bought_address
AND t1.blockchain = t2.blockchain
AND (t1.project = 'balancer' AND t2.project != 'balancer' 
OR t2.project = 'balancer' AND t1.project != 'balancer')
AND (t1.project = 'uniswap' AND t2.project != 'uniswap' 
OR t2.project = 'uniswap' AND t1.project != 'uniswap')
WHERE t1.blockchain = 'base'
AND t1.tx_hash NOT IN (SELECT DISTINCT tx_hash FROM dex_aggregator.trades WHERE blockchain = 'base')
),

routers as (
SELECT * FROM (values
(0xdef1c0ded9bec7f1a1670819833240f027b25eff, '0x'),
(0x1111111254eeb25477b68fb85ed929f73a960582, '1inch'),
(0xd857227a428babfa54d38e9da6b16893eadfad86, '1inch'),
(0x111111125421ca6dc452d289314280a0f8842a65, '1inch'),
(0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43, 'Aerodrome'),
(0x9008d19f58aabd9ed0d60971565aa8510560ab41, 'CoWSwap'),
(0xf9cFB8a62f50e10AdDE5Aa888B44cF01C5957055, 'Extra'),
(0x5334D0184a11f210De806Fcd5b556bf19981A7bE, 'FlatMoneyMM'),
(0x6131b5fae19ea4f9d964eac0408e4408b66337b5, 'Kyber'),
(0x19ceead7105607cd444f5ad10dd51356436095a1, 'Odos'),
(0xeDeAfdEf0901eF74Ee28c207BE8424D3B353D97A, 'Odos'),
(0x59c7c832e96d2568bea6db468c1aadcbbda08a52, 'Paraswap'),
(0x6a000f20005980200259b80c5102003040001068, 'Paraswap'),
(0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD, 'UniRouter'),
(0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24, 'UniRouter'),
(0x802b65b5d9016621E66003aeD0b16615093f328b, 'Arbitrage Bot'),
(0xec2e0f57fdc6bcdb5a2ea714fa6e1f8a17d6baf4, 'Arbitrage Bot'),
(0x6b2c0c7be2048daa9b5527982c29f48062b34d58, 'DEXRouter'),
(0x5965851f21dae82ea7c62f87fb7c57172e9f2add, 'DEXRouter'),
(0xba12222222228d8ba445958a75a0704d566bf2c8, 'Vault')
)
    as t (address, name))
    
SELECT al.address, al.name, al.blockchain FROM arbitrage_labels al
LEFT JOIN routers r ON al.address = r.address
WHERE r.address IS NULL
UNION ALL
SELECT address, name, 'base' as blockchain FROM routers
