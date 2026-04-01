-- part of a query repo
-- query name: aerodrome_sources_base
-- query link: https://dune.com/queries/3632646


WITH arbitrage_labels as
(
SELECT
    DISTINCT(CAST(t1.tx_to as varchar)) as address,
    'Arbitrage Bot' as name,
    t1.blockchain
FROM dex.trades t1
INNER JOIN dex.trades t2
ON t1.tx_hash = t2.tx_hash AND t1.token_bought_address = t2.token_sold_address
AND t1.token_sold_address = t2.token_bought_address
AND t1.blockchain = t2.blockchain
AND (t1.project = 'aerodrome' AND t2.project IN ('baseswap', 'uniswap','pancakeswap', 'sharkswap','balancer', 'rocketswap','maverick') 
OR t2.project = 'aerodrome' AND t1.project IN ('baseswap', 'uniswap','pancakeswap', 'sharkswap','balancer', 'rocketswap','maverick'))
WHERE t1.blockchain = 'base'
),

routers as (
SELECT * FROM (values
('0xdef1c0ded9bec7f1a1670819833240f027b25eff', '0x'),
('0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43', 'Aerodrome: Router'),
('0x1111111254eeb25477b68fb85ed929f73a960582', '1inch'),
('0xd857227a428babfa54d38e9da6b16893eadfad86', '1inch'),
('0x6131b5fae19ea4f9d964eac0408e4408b66337b5', 'Kyber'),
('0x19ceead7105607cd444f5ad10dd51356436095a1', 'Odos'),
('0x59c7c832e96d2568bea6db468c1aadcbbda08a52', 'Paraswap'),
('0xf9cFB8a62f50e10AdDE5Aa888B44cF01C5957055', 'Extra'),
('0x111111125421ca6dc452d289314280a0f8842a65', '1inch'),
('0xba12222222228d8ba445958a75a0704d566bf2c8', 'Vault')
)
    as t (address, name))
    
SELECT al.*, 'dapp usage' as "type", 'balancerlabs' as author FROM arbitrage_labels al
LEFT JOIN routers r ON al.address = lower(r.address)
WHERE lower(r.address) IS NULL
UNION ALL
SELECT *, 'base' as blockchain, 'balancer_source' as type, 'balancerlabs' as author FROM routers
