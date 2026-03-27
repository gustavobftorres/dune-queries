-- part of a query repo
-- query name: Daily % of Wrap/Unwrap Operations within Buffer Balance
-- query link: https://dune.com/queries/4457887


WITH txs_within_buffer AS (
  SELECT
    evt_block_time,
    COUNT(*) AS txs_in
  FROM query_4457927
  WHERE wrapped_balance > value
  AND wrappedToken = {{wrapped_token}}
  AND blockchain = '{{blockchain}}'
  GROUP BY evt_block_time
)

SELECT
  DATE_TRUNC('day', q.evt_block_time) AS block_date,
  CAST(COALESCE(SUM(t.txs_in), 0) AS DOUBLE) / COUNT(*) AS txs_in_ratio,
  COALESCE(SUM(t.txs_in), 0) AS txs_in,
  COUNT(*) AS txs
FROM query_4457927 AS q
LEFT JOIN txs_within_buffer AS t
  ON q.evt_block_time = t.evt_block_time
WHERE q.wrappedToken = {{wrapped_token}}
AND q.blockchain = '{{blockchain}}'
GROUP BY 1
ORDER BY block_date;
