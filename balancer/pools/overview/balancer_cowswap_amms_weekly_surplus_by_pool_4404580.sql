-- part of a query repo
-- query name: Balancer CoWSwap AMMs Weekly Surplus by Pool
-- query link: https://dune.com/queries/4404580


WITH t1 AS (
  SELECT
    DATE_TRUNC('week', time) AS block_date,
    blockchain,
    cow_amm_address,
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
  GROUP BY DATE_TRUNC('week', time), blockchain, cow_amm_address)
  
  
SELECT
  block_date,
      CASE 
        WHEN t.blockchain = 'arbitrum' THEN ' 🟦'
        WHEN t.blockchain = 'ethereum' THEN ' Ξ'
        WHEN t.blockchain = 'gnosis' THEN ' 🟩'
        WHEN t.blockchain = 'base' THEN ' 🟨'
      END
      || l.name
        AS pool,
  SUM(surplus + COALESCE(protocol_fee_usd, 0)) AS surplus
FROM t1 t
LEFT JOIN labels.balancer_cowswap_amm_pools l ON t.cow_amm_address = l.address
AND l.blockchain = t.blockchain
WHERE surplus > 0
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC