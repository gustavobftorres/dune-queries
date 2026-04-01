-- part of a query repo
-- query name: Distinct users on arbitrum
-- query link: https://dune.com/queries/3681920


SELECT
  block_date,
  project_contract_address,
  name,
  COUNT(DISTINCT tx_from) AS dau,
  COUNT(*) AS number_of_swaps,
  SUM(amount_usd) AS total_amount_usd
FROM balancer_v2_arbitrum.trades t
LEFT JOIN labels.balancer_v2_pools l ON t.project_contract_address = l.address AND l.blockchain = 'arbitrum'
WHERE DATE_TRUNC('day', block_time) BETWEEN TRY_CAST('2024-03-20' AS DATE) AND TRY_CAST('2024-03-30' AS DATE)
GROUP BY 1, 2, 3
ORDER BY 4 DESC