-- part of a query repo
-- query name: Top 100 daily volume by pool
-- query link: https://dune.com/queries/3671248


select block_date, blockchain, project_contract_address, sum(amount_usd) as daily_volume
from balancer.trades
group by 1,2,3
order by 4 desc
limit 100