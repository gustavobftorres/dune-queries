-- part of a query repo
-- query name: COW/WETH reCLAMM (Mainnet)
-- query link: https://dune.com/queries/5808136


select
    block_date,
    tvl_usd,
    swap_amount_usd as volume_usd,
    0.003 * swap_amount_usd as fees_usd,
    swap_amount_usd / tvl_usd as utilization,
    365 * 0.003 * swap_amount_usd / tvl_usd as apr
from balancer.pools_metrics_daily
where blockchain = 'ethereum'
and project_contract_address = 0xd321300ef77067d4a868f117d37706eb81368e98
and block_date < current_date
order by 1
