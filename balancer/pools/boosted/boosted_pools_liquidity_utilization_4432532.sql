-- part of a query repo
-- query name: Boosted Pools Liquidity Utilization
-- query link: https://dune.com/queries/4432532


SELECT
    block_date,
    SUM(swap_amount_usd) / SUM(tvl_usd) AS liq_util
FROM balancer.pools_metrics_daily m
INNER JOIN query_4419172 q ON m.project_contract_address = q.address
GROUP BY 1                                                                     