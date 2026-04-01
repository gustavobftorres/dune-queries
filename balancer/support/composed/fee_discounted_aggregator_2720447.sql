-- part of a query repo
-- query name: Fee Discounted Aggregator
-- query link: https://dune.com/queries/2720447


WITH fees AS (
  SELECT
    sum(amount_usd) AS amount_usd,
    date_trunc('day', block_time) AS day,
    blockchain,
    "Discount Reason",
    prev_fee * amount_usd AS no_discounted_total,
    fee * amount_usd AS discounted_total
  FROM query_2647345 f
  GROUP BY 2, 3, 4, 5, 6
),
last_cte AS (
  SELECT
    day,
    "Discount Reason",
    blockchain,
    COALESCE(sum(f.amount_usd), 0) AS "USD Amount",
    COALESCE(sum(no_discounted_total), 0) AS "not discounted",
    COALESCE(sum(discounted_total), 0) AS "discounted",
    COALESCE(sum(no_discounted_total), 0) - COALESCE(sum(discounted_total), 0) AS delta
  FROM fees f
  GROUP BY 1, 2, 3
)
SELECT
  day,
    blockchain || 
        CASE 
            WHEN blockchain = 'arbitrum' THEN ' 🟦'
            WHEN blockchain = 'avalanche_c' THEN ' ⬜ '
            WHEN blockchain = 'base' THEN ' 🟨'
            WHEN blockchain = 'ethereum' THEN ' Ξ'
            WHEN blockchain = 'gnosis' THEN ' 🟩'
            WHEN blockchain = 'optimism' THEN ' 🔴'
            WHEN blockchain = 'polygon' THEN ' 🟪'
        END 
    AS blockchain,
    "Discount Reason",
  SUM("USD Amount") AS "Volume",
  SUM(delta) AS "Aggregate Delta", --still needs fixing
  SUM(CASE WHEN day >= CURRENT_DATE - INTERVAL '1' DAY THEN delta ELSE 0 END) AS "Delta"
FROM last_cte
GROUP BY 1, 2, 3
ORDER BY 1 DESC;