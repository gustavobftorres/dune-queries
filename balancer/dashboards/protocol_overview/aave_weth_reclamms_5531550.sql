-- part of a query repo
-- query name: AAVE/WETH reCLAMMs
-- query link: https://dune.com/queries/5531550


select 
    block_date as day,
    case 
        when project_contract_address = 0x6cc9ef68864cd4c2af5a40ffb027c4b5428674a1 then 'boosted'
        when project_contract_address = 0x0180b025d911e88eb2db3fc7914140a737ab9f88 then 'boosted'
        when project_contract_address = 0xb847e40603aff979ff645f5a9a949d4ce80c3d01 then 'non-boosted'
    end as pool_type,
    sum(tvl_usd) as tvl,
    sum(swap_amount_usd) as volume,
    case 
        when sum(tvl_usd) > 1000 then sum(swap_amount_usd) / sum(tvl_usd)
        else null
    end as liquidity_utilization
from balancer.pools_metrics_daily
where project_contract_address in (
    0x6cc9ef68864cd4c2af5a40ffb027c4b5428674a1, -- boosted
    0x0180b025d911e88eb2db3fc7914140a737ab9f88, -- boosted
    0xb847e40603aff979ff645f5a9a949d4ce80c3d01  -- non-boosted
)
group by 1, 2
