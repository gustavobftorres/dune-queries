-- part of a query repo
-- query name: ReCLAMM Overall Metrics
-- query link: https://dune.com/queries/5057847


SELECT
  block_date,
  chain,
  pool,
  CAST(chain AS varchar) || ':' || SUBSTR(CAST(pool AS varchar), 1, 6) AS chain_pool,
  swap_amount_usd,
  tvl_usd
FROM balancer.pools_metrics_daily AS b
JOIN query_5057796 AS q
ON q.pool = b.project_contract_address AND q.chain = b.blockchain
