-- part of a query repo
-- query name: Balancer Exchange (Dune SQL)
-- query link: https://dune.com/queries/2829096


WITH
  swaps AS (
    SELECT
    CASE WHEN '{{1. Aggregation}}' = 'Daily' THEN date_trunc('day', block_time)
    WHEN '{{1. Aggregation}}' = 'Weekly' THEN date_trunc('week', block_time)
    WHEN '{{1. Aggregation}}' = 'Monthly' THEN date_trunc('month', block_time)
    END AS "date",
    version,
    COUNT(*) AS transactions,
    SUM(amount_usd) AS volume
    FROM balancer.trades
    WHERE ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
    AND block_time >= TIMESTAMP '{{2. Start date}}'
    AND block_time <= TIMESTAMP '{{3. End date}}' 
    AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
    GROUP BY 1, 2
  )
  
    SELECT
      "date",
      version,
      transactions,
      volume,
      volume / transactions
    FROM swaps