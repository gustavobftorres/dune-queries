-- part of a query repo
-- query name: AAVE/WETH reCLAMM Ethereum
-- query link: https://dune.com/queries/5622012


select
    block_date,
    tvl_usd,
    swap_amount_usd as volume_usd,
    0.0025 * swap_amount_usd as fees_usd,
    swap_amount_usd / tvl_usd as utilization,
    365 * 0.0025 * swap_amount_usd / tvl_usd as apr
from balancer.pools_metrics_daily
where blockchain = 'ethereum'
and project_contract_address = 0x9d1fcf346ea1b073de4d5834e25572cc6ad71f4d
and block_date < current_date
order by 1
