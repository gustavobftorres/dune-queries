-- part of a query repo
-- query name: Balancer Exchange (V3)
-- query link: https://dune.com/queries/4353327


WITH
  swaps AS (
    SELECT
    CASE WHEN '{{aggregation}}' = 'daily' THEN date_trunc('day', block_time)
    WHEN '{{aggregation}}' = 'weekly' THEN date_trunc('week', block_time)
    WHEN '{{aggregation}}' = 'monthly' THEN date_trunc('month', block_time)
    END AS "date",
    version,
    COUNT(*) AS transactions,
    SUM(amount_usd) AS volume
    FROM balancer.trades
    WHERE ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
    AND block_time >= TIMESTAMP '2024-11-29 00:00'
    GROUP BY 1, 2
  )
  
    SELECT
      "date",
      version,
      transactions,
      volume,
      volume / transactions
    FROM swaps