-- part of a query repo
-- query name: (query_ 3699365) balancer_sources_zkevm
-- query link: https://dune.com/queries/3699365


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
WHERE t1.blockchain = 'zkevm'
AND t1.tx_hash NOT IN (SELECT DISTINCT tx_hash FROM dex_aggregator.trades WHERE blockchain = 'zkevm')
),


routers as (
SELECT * FROM (values
(0x7b2e3fc7510d1a51b3bef735f985446589219354, 'InterPort'),
(0x6dd434082eab5cd134b33719ec1ff05fe985b97b, 'OpenOcean'),
(0xba12222222228d8ba445958a75a0704d566bf2c8, 'Vault')
)
    as t (address, name))
    
SELECT al.* FROM arbitrage_labels al
LEFT JOIN routers r ON al.address = r.address
WHERE r.address IS NULL
UNION ALL
SELECT address, name, 'zkevm' as blockchain FROM routers
