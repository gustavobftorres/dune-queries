-- part of a query repo
-- query name: Volume by chain, project and aggregator
-- query link: https://dune.com/queries/5534523


SELECT 
    dt.*,
    dat.project as aggregator_project
FROM dex.trades dt
LEFT JOIN dex_aggregator.trades dat 
    ON dt.tx_hash = dat.tx_hash
WHERE dt.block_time > NOW() - INTERVAL '{{days}}' day
and dt.blockchain = 'base'
and dt.project = 'balancer'
--and dat.project = '1inch'
--or dat.project = 'kyberswap'
--or dat.project = 'paraswap'
--or dat.project = '0x API'
--or dat.project = 'bebop'
--or dat.project = 'cow_protocol'
--or dat.project = 'odos'
--or dat.project = 'velora_delta'
