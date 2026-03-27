-- part of a query repo
-- query name: Balancer Sankey
-- query link: https://dune.com/queries/3150087


select
  *
from
  dune.flashbots.result_overall_lq
where
  liquidity_src = 'Balancer'