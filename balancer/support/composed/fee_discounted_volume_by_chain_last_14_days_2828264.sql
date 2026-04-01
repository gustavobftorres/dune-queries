-- part of a query repo
-- query name: Fee Discounted Volume by chain, last 14 days
-- query link: https://dune.com/queries/2828264


WITH calendar AS (
  SELECT date_sequence AS day
  FROM unnest(sequence(date(now() - interval '14' day), date(now()), interval '1' day)) AS t(date_sequence)
),
fees AS (
  SELECT sum(amount_usd) AS amount_usd, date_trunc('day', block_time) AS day, blockchain,
        "Discount Reason", prev_fee*amount_usd as no_discounted_total, fee*amount_usd as discounted_total
  FROM query_2647345 f 
  WHERE ('{{3. Blockchain}}' = 'All' OR blockchain = '{{3. Blockchain}}')
  GROUP BY 2,3,4,5,6
),
last_cte as (SELECT 
        CAST(c.day AS timestamp) AS day, 
        --"Discount Reason",
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
    COALESCE(sum(f.amount_usd), 0) AS "USD Amount",
        COALESCE(sum(no_discounted_total), 0) AS "not discounted",
        COALESCE(sum(discounted_total), 0) AS "discounted",
        COALESCE(sum(no_discounted_total), 0) - COALESCE(sum(discounted_total), 0) AS delta
FROM calendar c
LEFT JOIN fees f ON CAST(c.day AS timestamp) = f.day
GROUP BY 1,2
ORDER BY 1)

SELECT *,  SUM(delta) OVER (ORDER BY day,/*"Discount Reason",*/ "blockchain" ASC) AS "Accumulated Delta" from last_cte