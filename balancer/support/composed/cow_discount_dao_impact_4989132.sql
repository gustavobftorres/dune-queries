-- part of a query repo
-- query name: Cow Discount DAO impact
-- query link: https://dune.com/queries/4989132


SELECT
  blockchain,
  EXTRACT(YEAR FROM block_time) AS year,
  CASE
    WHEN tx_to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41 THEN 'CowSwap'
    WHEN tx_to = 0xad3b67bca8935cb510c8d18bd45f0b94f54a968f THEN '1inch'
    ELSE "Discount Reason"
  END AS "Discount Reason",
  amount_usd,
  prev_fee,
  fee,
  (prev_fee - fee) AS used_fee,
  (amount_usd * (prev_fee - fee)) AS total_discount,
  ((amount_usd * (prev_fee - fee)) * 0.5) AS protocol_fee_discount,
  (((amount_usd * (prev_fee - fee)) * 0.5) * 0.175) AS DAO_discount
FROM query_2647345
ORDER BY block_time DESC;