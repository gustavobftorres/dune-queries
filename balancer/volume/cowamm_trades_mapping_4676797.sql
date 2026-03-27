-- part of a query repo
-- query name: CoWAMM Trades Mapping
-- query link: https://dune.com/queries/4676797


WITH base_table_mapping AS( --with data from https://dune.com/queries/3964868
WITH t1 AS (
  SELECT
    tx_hash,
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
  AND blockchain = 'gnosis'
  AND cow_amm_address = 0x5089007dec8e93f891dcb908c9e2af8d9dedb72e
  GROUP BY 1)
  
  
SELECT
  tx_hash,
  SUM(surplus + COALESCE(protocol_fee_usd, 0)) AS surplus
FROM t1
WHERE surplus >= 0 
GROUP BY 1
)

SELECT
     block_time,
     block_number,
     evt_index,
     t.tx_hash,
     sell_token_address,
     sell_token,
     buy_token_address,
     buy_token,
     units_sold,
     units_bought,
     buy_price,
     sell_price,
     buy_value_usd,
     sell_value_usd,
     surplus_usd AS cow_spell_surplus,
     b.surplus AS base_table_surplus,
     ABS(surplus_usd - b.surplus) AS absolute_surplus_delta
FROM cow_protocol_gnosis.trades t
LEFT JOIN base_table_mapping b ON t.tx_hash = b.tx_hash
WHERE trader = 0x5089007dec8e93f891dcb908c9e2af8d9dedb72e
ORDER BY 17 DESC