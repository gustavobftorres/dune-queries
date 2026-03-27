-- part of a query repo
-- query name: Boosted Pools Competition Snaphot
-- query link: https://dune.com/queries/4419214


SELECT
    lending_market,
    SUM(CASE WHEN block_date = CURRENT_DATE THEN tvl_usd ELSE 0 END) AS tvl,
    SUM(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' day THEN swap_amount_usd ELSE 0 END) AS volume
FROM balancer.pools_metrics_daily m
INNER JOIN query_4419172 q ON m.project_contract_address = q.address
GROUP BY 1    