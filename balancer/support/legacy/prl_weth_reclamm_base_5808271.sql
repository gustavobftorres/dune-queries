-- part of a query repo
-- query name: PRL/WETH reCLAMM (Base)
-- query link: https://dune.com/queries/5808271


select
    block_date,
    tvl_usd,
    swap_amount_usd as volume_usd,
    0.003 * swap_amount_usd as fees_usd,
    swap_amount_usd / tvl_usd as utilization,
    365 * 0.003 * swap_amount_usd / tvl_usd as apr
from balancer.pools_metrics_daily
where blockchain = 'base'
and project_contract_address = 0x53d31feb99eccd1375a9ec433d1d7873dfb68263
and block_date < current_date
order by 1
