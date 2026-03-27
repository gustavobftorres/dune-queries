-- part of a query repo
-- query name: wstETH Collected by Balancer's Protocol Fee
-- query link: https://dune.com/queries/1130743


with foo as (
    select date_trunc('week', evt_block_time) as week, sum(value/1e18) as amount
    from erc20."ERC20_evt_Transfer"
    where contract_address = '\x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0'
    and "to" = '\xce88686553686da562ce7cea497ce749da109f9f'
    group by 1
)

select *, sum(amount) over (order by week) as cumulative
from foo