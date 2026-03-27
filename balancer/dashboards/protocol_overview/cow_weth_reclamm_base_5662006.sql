-- part of a query repo
-- query name: COW/WETH reCLAMM (Base)
-- query link: https://dune.com/queries/5662006


select
    block_date,
    tvl_usd,
    swap_amount_usd as volume_usd,
    0.003 * swap_amount_usd as fees_usd,
    swap_amount_usd / tvl_usd as utilization,
    365 * 0.003 * swap_amount_usd / tvl_usd as apr
from balancer.pools_metrics_daily
where blockchain = 'base'
and project_contract_address = 0xff028c1ec4559d3aa2b0859aa582925b5cc28069
and block_date < current_date
order by 1
