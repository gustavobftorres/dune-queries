-- part of a query repo
-- query name: Balancer Volume by Token
-- query link: https://dune.com/queries/2628941


WITH swaps AS (
  SELECT
      CASE
          WHEN '{{Aggregation}}' = 'Last 24 hours' THEN date_trunc('hour', d.block_time)
          WHEN '{{Aggregation}}' = 'Last 30 days' THEN date_trunc('day', d.block_time)
          WHEN '{{Aggregation}}' = 'Last 90 days' THEN date_trunc('week', d.block_time)
          WHEN '{{Aggregation}}' = 'Last 365 days' THEN date_trunc('month', d.block_time)
          WHEN '{{Aggregation}}' = 'All-Time' THEN date_trunc('month', d.block_time)
      END AS time,
      sum(amount_usd) AS volume,
      CAST(d.token_bought_address AS varchar) AS address,
            CONCAT( CASE 
            WHEN t.blockchain = 'arbitrum' THEN ' 🟦 |'
            WHEN t.blockchain = 'avalanche_c' THEN ' ⬜  |'
            WHEN t.blockchain = 'base' THEN ' 🟨 |'
            WHEN t.blockchain = 'ethereum' THEN ' Ξ |'
            WHEN t.blockchain = 'gnosis' THEN ' 🟩 |'
            WHEN t.blockchain = 'optimism' THEN ' 🔴 |'
            WHEN t.blockchain = 'polygon' THEN ' 🟪 |'
            WHEN t.blockchain = 'zkevm' THEN ' 🟣 |'
            END 
            , ' ', t.symbol) AS token
  FROM dex.trades d
  LEFT JOIN tokens.erc20 t ON CAST(t.contract_address AS varchar) = CAST(d.token_bought_address AS varchar)
  AND d.blockchain = t.blockchain
  WHERE project = 'balancer' AND (('{{4. Blockchain}}' = 'All' AND 1 = 1) OR d.blockchain = '{{4. Blockchain}}')
    AND (
      ('{{Aggregation}}' = 'Last 24 hours' AND date_trunc('hour', d.block_time) >= date_trunc('hour', now() - interval '1' day)) OR
      ('{{Aggregation}}' = 'Last 30 days' AND date_trunc('day', d.block_time) >= date_trunc('day', now() - interval '1' month)) OR
      ('{{Aggregation}}' = 'Last 90 days' AND date_trunc('week', d.block_time) >= date_trunc('week', now() - interval '3' month)) OR
      ('{{Aggregation}}' = 'Last 365 days' AND date_trunc('month', d.block_time) >= date_trunc('week', now() - interval '1' year))
    )
    AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
  GROUP BY 1, 3, 4
),
ranking AS (
  SELECT
    s.time,
    COALESCE(s.token, CONCAT(SUBSTRING(s.address, 3, 6), '...')) AS token,
    s.address,
    ROW_NUMBER() OVER (PARTITION BY s.time ORDER BY SUM(s.volume) DESC NULLS LAST) AS position,
    SUM(s.volume) / 2 AS volume
  FROM swaps s
  GROUP BY 1, 2, 3
  ORDER BY 1, 3
)
SELECT *
FROM ranking
WHERE position <= {{Top x Tokens}};
