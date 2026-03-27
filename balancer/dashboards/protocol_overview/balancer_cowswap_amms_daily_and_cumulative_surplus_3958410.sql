-- part of a query repo
-- query name: Balancer CoWSwap AMMs Daily and Cumulative Surplus
-- query link: https://dune.com/queries/3958410


WITH t1 AS (
  SELECT
    DATE_TRUNC('day', time) AS block_date,
    SUM(
      CASE
        WHEN token_1_transfer_usd > 0
        THEN (
          token_1_transfer_usd + token_1_balance_usd * token_2_transfer_usd / (
            token_2_balance_usd - token_2_transfer_usd))
        ELSE (
          token_2_transfer_usd + token_2_balance_usd * token_1_transfer_usd / (
            token_1_balance_usd - token_1_transfer_usd ))
      END) AS surplus,
    SUM(protocol_fee_usd) AS protocol_fee_usd
  FROM dune.balancer.result_b_cow_amm_base_table
  WHERE istrade
  AND time >=   TIMESTAMP '{{1. Start date}}'         
  AND ('{{2. Blockchain}}' = 'All' OR blockchain = '{{2. Blockchain}}')
  GROUP BY DATE_TRUNC('day', time))
  
  
SELECT
  block_date,
  SUM(surplus + COALESCE(protocol_fee_usd, 0)) AS surplus,
  SUM(SUM(surplus + COALESCE(protocol_fee_usd, 0))) OVER (ORDER BY block_date) AS cumulative_surplus
FROM t1
WHERE surplus > 0
GROUP BY block_date