-- part of a query repo
-- query name: Balancer CoWSwap AMMs Daily Surplus by Blockchain
-- query link: https://dune.com/queries/4075127


WITH t1 AS (
  SELECT
    DATE_TRUNC('day', time) AS block_date,
    blockchain,
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
  GROUP BY DATE_TRUNC('day', time), blockchain)
  
  
SELECT
  block_date,
  blockchain || 
      CASE 
        WHEN blockchain = 'arbitrum' THEN ' 🟦'
        WHEN blockchain = 'ethereum' THEN ' Ξ'
        WHEN blockchain = 'gnosis' THEN ' 🟩'
        WHEN blockchain = 'base' THEN ' 🟨'
      END 
        AS blockchain,
  SUM(surplus + COALESCE(protocol_fee_usd, 0)) AS surplus
FROM t1
WHERE surplus > 0
GROUP BY 1, 2