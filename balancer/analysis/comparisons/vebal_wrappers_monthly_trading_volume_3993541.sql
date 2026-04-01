-- part of a query repo
-- query name: veBAL Wrappers Monthly Trading Volume
-- query link: https://dune.com/queries/3993541


SELECT
  DATE_TRUNC('month', block_date) AS block_date,
  'auraBAL' AS symbol,
  SUM(
    CASE
      WHEN NOT amount_usd IS NULL
      THEN amount_usd
      WHEN amount_usd IS NULL AND token_bought_address = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56
      THEN token_bought_amount * bpt_price
      WHEN amount_usd IS NULL AND token_sold_address = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56
      THEN token_sold_amount * bpt_price
      ELSE 0
    END
  ) AS volume
FROM dex.trades AS t
LEFT JOIN balancer.bpt_prices AS p
  ON t.block_date = p.day
  AND p.blockchain = 'ethereum'
  AND p.contract_address = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56
WHERE (t.blockchain = 'ethereum'
    AND (token_bought_address = 0x616e8BfA43F920657B3497DBf40D6b1A02D4608d
      OR token_sold_address = 0x616e8BfA43F920657B3497DBf40D6b1A02D4608d))
    AND block_time BETWEEN timestamp '{{Start Date}}' and timestamp '{{End Date}}'
GROUP BY 1, 2

UNION ALL

SELECT
  DATE_TRUNC('month', block_date) AS block_date,
  'sdBAL' AS symbol,
  SUM(
    CASE
      WHEN NOT amount_usd IS NULL
      THEN amount_usd
      WHEN amount_usd IS NULL AND token_bought_address = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56
      THEN token_bought_amount * bpt_price
      WHEN amount_usd IS NULL AND token_sold_address = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56
      THEN token_sold_amount * bpt_price
      ELSE 0
    END
  ) AS volume
FROM dex.trades AS t
LEFT JOIN balancer.bpt_prices AS p ON t.block_date = p.day
  AND p.blockchain = 'ethereum'
  AND p.contract_address = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56
WHERE (t.blockchain = 'ethereum'
    AND (token_bought_address = 0xf24d8651578a55b0c119b9910759a351a3458895
      OR token_sold_address = 0xf24d8651578a55b0c119b9910759a351a3458895))
    AND block_time BETWEEN timestamp '{{Start Date}}' and timestamp '{{End Date}}'
GROUP BY 1, 2

UNION ALL

SELECT
  DATE_TRUNC('month', block_date) AS block_date,
  'tetuBAL' AS symbol,
  SUM(
    CASE
      WHEN NOT amount_usd IS NULL
      THEN amount_usd
      WHEN amount_usd IS NULL AND token_bought_address = 0x3d468ab2329f296e1b9d8476bb54dd77d8c2320f
      THEN token_bought_amount * bpt_price
      WHEN amount_usd IS NULL AND token_sold_address = 0x3d468ab2329f296e1b9d8476bb54dd77d8c2320f
      THEN token_sold_amount * bpt_price
      ELSE 0
    END
  ) AS volume
FROM dex.trades t
LEFT JOIN balancer.bpt_prices AS p ON t.block_date = p.day
  AND p.blockchain = 'ethereum'
  AND p.contract_address = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56
WHERE(t.blockchain = 'polygon'
    AND (
      token_bought_address = 0x7fc9e0aa043787bfad28e29632ada302c790ce33
      OR token_sold_address = 0x7fc9e0aa043787bfad28e29632ada302c790ce33))
    AND block_time BETWEEN timestamp '{{Start Date}}' and timestamp '{{End Date}}'
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC