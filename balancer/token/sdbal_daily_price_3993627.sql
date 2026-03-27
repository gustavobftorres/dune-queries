-- part of a query repo
-- query name: sdBAL daily price
-- query link: https://dune.com/queries/3993627


WITH price_per_tx AS(
SELECT
  block_time,
  'sdBAL' AS symbol,
    CASE
      WHEN NOT amount_usd IS NULL
      THEN amount_usd
      WHEN amount_usd IS NULL AND token_bought_address = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56
      THEN (token_bought_amount * bpt_price) / token_sold_amount
      WHEN amount_usd IS NULL AND token_sold_address = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56
      THEN (token_sold_amount * bpt_price) / token_bought_amount
      ELSE 0
    END AS sdbal_price
FROM dex.trades AS t
LEFT JOIN balancer.bpt_prices AS p ON t.block_date = p.day
  AND p.blockchain = 'ethereum'
  AND p.contract_address = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56
WHERE (t.blockchain = 'ethereum'
    AND (token_bought_address = 0xf24d8651578a55b0c119b9910759a351a3458895
      OR token_sold_address = 0xf24d8651578a55b0c119b9910759a351a3458895)))
      
SELECT
    DATE_TRUNC('day', block_time) AS day,
    APPROX_PERCENTILE(sdbal_price,0.5) AS median_price
FROM price_per_tx
GROUP BY 1