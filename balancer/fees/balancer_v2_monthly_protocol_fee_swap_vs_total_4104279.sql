-- part of a query repo
-- query name: Balancer V2 Monthly Protocol Fee (Swap vs Total)
-- query link: https://dune.com/queries/4104279


with swap_fees as (
    select block_month, blockchain, project_contract_address, sum(amount_usd * swap_fee) * 0.5 as total_protocol_swap_fee
    from balancer.trades
    where version = '2'
    group by 1, 2, 3
),

pool_metrics as (
    select DATE_TRUNC('month', block_date) AS block_month, blockchain, pool_symbol, pool_type, project_contract_address, sum(fee_amount_usd) as total_protocol_fee_collected
    from balancer.pools_metrics_daily
    where version = '2'
    group by 1, 2, 3, 4, 5
)

select p.block_month, p.blockchain, p.pool_symbol, p.pool_type, p.project_contract_address as pool_address, total_protocol_fee_collected, total_protocol_swap_fee, total_protocol_fee_collected - total_protocol_swap_fee AS total_protocol_yield_fee
from pool_metrics p
left join swap_fees s
on s.blockchain = p.blockchain
and s.project_contract_address = p.project_contract_address
AND s.block_month = p.block_month
order by 1 DESC, 6 desc
