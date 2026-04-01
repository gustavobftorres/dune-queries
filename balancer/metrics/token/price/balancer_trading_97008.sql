-- part of a query repo
-- query name: Balancer Trading
-- query link: https://dune.com/queries/97008


WITH
  balancer_trades AS (
  SELECT
    blockchain,
    block_time,
    amount_usd,
    swap_fee
  FROM balancer.trades
  WHERE ('{{5. Version}}' = 'All' OR '{{5. Version}}' = version)
  ),
  
  revenues_volume AS (
    SELECT
      DATE_TRUNC('week', block_time) AS week,
      SUM(amount_usd * swap_fee) AS revenues,
      SUM(amount_usd) AS volume,
      COUNT(*) AS n_swaps
    FROM balancer_trades
    WHERE ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
    GROUP BY 1
  ),
  
  cumulative_metrics AS (
    SELECT
      week,
      n_swaps,
      volume,
      revenues,
      SUM(n_swaps) OVER (ORDER BY week) AS cumulative_swaps,
      SUM(volume) OVER (ORDER BY week) AS cumulative_volume,
      SUM(revenues) OVER (ORDER BY week) AS cumulative_revenues
    FROM revenues_volume
  )
SELECT *
FROM cumulative_metrics
ORDER BY 1