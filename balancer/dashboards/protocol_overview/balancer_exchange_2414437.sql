-- part of a query repo
-- query name: Balancer Exchange
-- query link: https://dune.com/queries/2414437


WITH
  swaps AS (
    SELECT
    CASE WHEN '{{Aggregation}}' = 'Daily' THEN date_trunc('day', block_time)
            WHEN '{{Aggregation}}' = 'Weekly' THEN date_trunc('week', block_time)
            end as "date",
      CONCAT('V', version) AS version,
      COUNT(*) AS transactions,
      SUM(CAST(amount_usd AS DOUBLE)) AS volume
    FROM
      dex.trades
    WHERE
      project = 'balancer' 
        AND ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
    GROUP BY
      1,
      2
  )
SELECT
  "date",
  version,
  transactions,
  volume,
  volume / transactions
FROM
  swaps