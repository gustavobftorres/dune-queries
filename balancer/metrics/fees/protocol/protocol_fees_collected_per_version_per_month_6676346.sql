-- part of a query repo
-- query name: Protocol fees collected per version per month
-- query link: https://dune.com/queries/6676346


select
  date_trunc('month', day) as month,
  version,
  sum(protocol_fee_collected_usd) as total_protocol_fee_collected_usd
from balancer.protocol_fee
where day >= date '2025-01-01'
group by 1, 2
order by 1, 2;