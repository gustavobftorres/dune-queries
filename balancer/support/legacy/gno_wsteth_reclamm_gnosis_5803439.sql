-- part of a query repo
-- query name: GNO/wstETH reCLAMM (Gnosis)
-- query link: https://dune.com/queries/5803439


select
    block_date,
    tvl_usd,
    swap_amount_usd as volume_usd,
    0.003 * swap_amount_usd as fees_usd,
    swap_amount_usd / tvl_usd as utilization,
    365 * 0.003 * swap_amount_usd / tvl_usd as apr
from balancer.pools_metrics_daily
where blockchain = 'gnosis'
and project_contract_address = 0xa50085ff1dfa173378e7d26a76117d68d5eba539
and block_date < current_date
order by 1
