-- part of a query repo
-- query name: (query_2999200) balancer_sources_optimism
-- query link: https://dune.com/queries/2999200


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
WHERE t1.blockchain = 'optimism'
AND t1.tx_hash NOT IN (SELECT DISTINCT tx_hash FROM dex_aggregator.trades WHERE blockchain = 'optimism')
),

routers as (
SELECT * FROM (values
(0x1111111254760f7ab3f16433eea9304126dcd199, '1inch'),
(0x1111111254eeb25477b68fb85ed929f73a960582, '1inch'),
(0xbc2ff189e0349ca73d9b78c172fc2b40025abe2a, 'Aave'),
(0x794a61358d6845594f94dc1db02a252b5b4814ad, 'Aave'),
(0xf762c3fc745948ff49a3da00ccdc6b755e44305e, 'Chainhop'),
(0x49994fbb41c6b7c2f3066ca7301f25305de1234a, 'Chainhop'),
(0x716fcc67dca500a91b4a28c9255262c398d8f971, 'DODO'),
(0xfd9d2827ad469b72b69329daa325ba7afbdb3c98, 'DODO'),
(0x1dd674678c80cbd2af951437c723e8b45f54e46c, 'Gnosis Safe'),
(0x0d9b71891dc86400acc7ead08c80af301ccb3d71, 'Gnosis Safe'),
(0x3aecb7a1705e350c7bbf3c7719afdbb33f08ce83, 'Gnosis Safe'),
(0xfb9423283eb7f65210b9ab545ecc212b5ae52b3a, 'Gnosis Safe'),
(0x07c9fb5c7304b37b9a9bd4c8fadf2e042b9a4725, 'Gnosis Safe'),
(0xf6fd4c5cb0d2a92fbf8e08e6c2a27ca7fe39fdcc, 'Gnosis Safe'),
(0x375f6b0cd12b34dc28e34c26853a37012c24dde5, 'Gnosis Safe'),
(0x7bf4e079c6ae785e3d87ae916b8263289421411e, 'Gnosis Safe'),
(0xc30141b657f4216252dc59af2e7cdb9d8792e1b0, 'Gnosis Safe'),
(0x6a000f20005980200259b80c5102003040001068, 'Paraswap'),
(0x69dd38645f7457be13571a847ffd905f9acbaf6d, 'ODOS'),
(0x103ba715376ce6116be51e3317fbbf3c4128c498, 'Overnight'),
(0x11732e21d9dab3b6ff6e7dd9edcb24770260c7b4, 'Overnight'),
(0xe00d67f732e6ed1158553fdbe9c6a151d06bed6c, 'Socket'),
(0x103d0634ec6c9e1f633381b16f8e2fe56a2e7372, 'Unidex'),
(0xba12222222228d8ba445958a75a0704d566bf2c8, 'Vault'),
(0xa062ae8a9c5e11aaa026fc2670b0d65ccc8b2858, 'Velodrome: Router'),
(0xf132bdb9573867cd72f2585c338b923f973eb817, 'Velodrome: Router'),
(0xdef1abe32c034e558cdd535791643c58a13acc10, '0x'),
(0xca423977156bb05b13a2ba3b76bc5419e2fe9680, 'ODOS'),
(0xf9cfb8a62f50e10adde5aa888b44cf01c5957055, 'VeloPositionManager'),
(0xdef171fe48cf0115b1d80b88dc8eab59176fee57, 'Paraswap'),
(0x111111125421ca6dc452d289314280a0f8842a65, '1inch'),
(0x6131b5fae19ea4f9d964eac0408e4408b66337b5, 'Kyber')
)
    as t (address, name))
    
SELECT al.* FROM arbitrage_labels al
LEFT JOIN routers r ON al.address = r.address
WHERE r.address IS NULL
UNION ALL
SELECT address, name, 'optimism' as blockchain FROM routers
