-- part of a query repo
-- query name: (query_2998828) balancer_sources_arbitrum
-- query link: https://dune.com/queries/2998828


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
WHERE t1.blockchain = 'arbitrum'
AND t1.tx_hash NOT IN (SELECT DISTINCT tx_hash FROM dex_aggregator.trades WHERE blockchain = 'arbitrum')
),

routers as (
SELECT * FROM (values
(0xdef1c0ded9bec7f1a1670819833240f027b25eff, '0x'),
(0xe9da98417e01ab72f50b522a52dfd0eb4dc9931b, '0x'),
(0xad3b67bca8935cb510c8d18bd45f0b94f54a968f, '1inch'),
(0x1111111254fb6c44bac0bed2854e76f90643097d, '1inch'),
(0x1111111254eeb25477b68fb85ed929f73a960582, '1inch'),
(0x111111125421cA6dc452d289314280a0f8842A65, '1inch'),
(0x726413d7402ff180609d0ebc79506df8633701b1, 'Abracadabra'),
(0x794a61358d6845594f94dc1db02a252b5b4814ad, 'Aave'),
(0xba7bAC71a8Ee550d89B827FE6d67bc3dCA07b104, 'Diamond'),
(0x9008d19f58aabd9ed0d60971565aa8510560ab41, 'CoWSwap'),
(0x88cbf433471a0cd8240d2a12354362988b4593e5, 'DODO'),
(0x4775af8fef4809fe10bf05867d2b038a4b5b2146, 'Gelato'),
(0x3e7a97005342b25538412c62ee8a31c425045169, 'Gnosis Safe'),
(0x97893012fbe4ff00dfb18871e7dd7f6394711150, 'Gnosis Safe'),
(0xe5011a7cc5cda29f02ce341b2847b58abefa7c26, 'Gnosis Safe'),
(0x7a70ed92b61836e90d99e6c7bc285880ba0a64cb, 'Gnosis Safe'),
(0x8f8d73b7c19cebf904013cf900ec84561e78a4a3, 'Gnosis Safe'),
(0x13556651ac14db8218d298a0f7b90481dda08891, 'Gnosis Safe'),
(0xfb1653f69927b001c63f691ff8bd33eb65b9166b, 'Gnosis Safe'),
(0x0bf63669d737701abad92854de8ef580197ad780, 'Gnosis Safe'),
(0x265b6c4e4f704344e359a3f472a654c54940ba49, 'Gnosis Safe'),
(0xcc6e3530f2c25803c1f548f649c5ef8f0ceb2bca, 'Gnosis Safe'),
(0x5f96609fcb00da05d3b686bc59da8916af7c184b, 'Gnosis Safe'),
(0x000000000002733be21c62d22209523701bd6bc1, 'Gnosis Safe'),
(0x000000000039da17e6661018b30a8af5119bde05, 'Gnosis Safe'),
(0xaaaa55c44c7b4d87662fc8ff1f310c96ed57ce1f, 'Gnosis Safe'),
(0x0000000000dce9a62708c6361f703ca0ae4b69f4, 'Gnosis Safe'),
(0x40a2accbd92bca938b02010e17a5b8929b49130d, 'Gnosis Safe'),
(0x49347ccca8f067602d32b3aead1dfae44a2f02f9, 'Gnosis Safe'),
(0x375f6b0cd12b34dc28e34c26853a37012c24dde5, 'Gnosis Safe'),
(0x4f49d4746fce836787c86b0a059fabfedc601318, 'Gnosis Safe'),
(0x7241034885cf869ebeef8aeb014b7f911f95c267, 'Gnosis Safe'),
(0x68243c907e4e39e5e1630ea7f99760bf8ad61ac3, 'Gnosis Safe'),
(0x89c6340b1a1f4b25d36cd8b063d49045caf3f818, 'LI.FI'),
(0xfB1B08BA6BA284934D817Ea3C9D18f592cc59a50, 'Magpie'),
(0xa19fd5ab6c8dcffa2a295f78a5bb4ac543aaf5e3, 'MUX'),
(0xdd94018f54e565dbfc939f7c44a16e163faab331, 'ODOS'),
(0xa669e7A0d4b3e4Fa48af2dE86BD4CD7126Be4e13, 'ODOS'),
(0x00000000005bbb0ef59571e58418f9a4357b68a0, 'Pendle'),
(0x6a000f20005980200259b80c5102003040001068, 'Paraswap'),
(0xbbbfd134e9b44bfb5123898ba36b01de7ab93d98, 'Relay'),
(0xf5042e6ffac5a625d4e7848e0b01373d8eb9e222, 'Relay'),
(0x8b14984de0ddd2e080d8679febe2f5c94b975af8, 'Socket'),
(0xb7e50106a5bd3cf21af210a755f9c8740890a8c9, 'Sushi'),
(0x7050a8908e2a60899d8788015148241f0993a3fd, 'Sushi'),
(0x588948240b55b425eb6c8c017028f7580cd2f3b5, 'Sushi'),
(0x9745e5cc0522827958ee3fc2c03247276d359186, 'Sushi'),
(0x905dfcd5649217c42684f23958568e533c711aa3, 'Sushi'),
(0x515e252b2b5c22b4b2b6df66c2ebeea871aa4d69, 'Sushi'),
(0xb329504622bd79329c6f82cf8c60c807df2090c4, 'Strategy'),
(0x103d0634ec6c9e1f633381b16f8e2fe56a2e7372, 'Unidex'),
(0xc36442b4a4522e871399cd717abdd847ab11fe88, 'Uniswap V3'),
(0xba12222222228d8ba445958a75a0704d566bf2c8, 'Vault'),
(0xdef171fe48cf0115b1d80b88dc8eab59176fee57, 'Paraswap'),
(0x9dda6ef3d919c9bc8885d5560999a3640431e8e6, 'MetaMask'),
(0xedeafdef0901ef74ee28c207be8424d3b353d97a, 'Odos'),
(0x0d05a7d3448512b78fa8a9e46c4872c88c4a0d05, 'Odos')
--(0xe16e2f35da363a4bd330812e7cffb3f51a97c7d1, 'ERC1967 proxu'),
)
    as t (address, name))
    
SELECT al.* FROM arbitrage_labels al
LEFT JOIN routers r ON al.address = r.address
WHERE r.address IS NULL
UNION ALL
SELECT address, name, 'arbitrum' as blockchain FROM routers
