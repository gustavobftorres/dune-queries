-- part of a query repo
-- query name: DEXs YTD Volume by CoW Solver (Base)
-- query link: https://dune.com/queries/5175072


select block_month, s.address as solver_address, s.name as solver_name, sum(amount_usd) as volume
from dex.trades t
inner join cow_protocol_{{blockchain}}.solvers s
on t.tx_to = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41
and t.tx_from = s.address
and s.environment = 'prod'
and t.block_month >= timestamp '2025-01-01'
and t.block_month <= timestamp '2025-12-01'
and t.blockchain = '{{blockchain}}'
group by 1, 2, 3
order by 1, 2, 3, 4 desc
