-- part of a query repo
-- query name: MEVCapture hook daily and cumulative MEV Captured
-- query link: https://dune.com/queries/4734857


SELECT
  block_date,
  SUM(mev_captured) AS mev_captured,
  SUM(SUM(mev_captured)) OVER (ORDER BY block_date) AS cumulative_mev_captured
FROM query_4734806
WHERE mev_captured > 0
GROUP BY block_date