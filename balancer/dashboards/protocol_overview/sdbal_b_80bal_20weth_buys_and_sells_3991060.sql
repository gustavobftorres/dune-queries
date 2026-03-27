-- part of a query repo
-- query name: sdBAL B-80BAL-20WETH buys and sells
-- query link: https://dune.com/queries/3991060


WITH sdBAL_buys AS (
  SELECT
    DATE_TRUNC('day', block_time) AS block_time,
    (
      CAST(token_bought_amount_raw as DOUBLE) / token_sold_amount_raw
    ) AS price,
    true as is_buying_sd,
    token_bought_amount + token_sold_amount AS amount,
    tx_hash AS evt_tx_hash,
    token_bought_amount AS buys,
    -token_sold_amount AS sells,
    token_bought_amount,
    token_sold_amount
  FROM balancer_v2_ethereum.trades
  WHERE
    pool_id = 0x2d011adf89f0576c9b722c28269fcb5d50c2d17900020000000000000000024d
    AND token_bought_address = 0xf24d8651578a55b0c119b9910759a351a3458895
    AND block_time BETWEEN timestamp '{{Start Date}}' and timestamp '{{End Date}}'
),
sdBAL_sells AS (
  SELECT
    DATE_TRUNC('day', block_time) AS block_time,
    (
      CAST(token_bought_amount_raw as DOUBLE) / token_sold_amount_raw
    ) AS price,
    false as is_buying_sd,
    token_bought_amount + token_sold_amount AS amount,
    tx_hash AS evt_tx_hash,
    token_bought_amount AS buys,
    -token_sold_amount AS sells,
    token_bought_amount,
    token_sold_amount
  FROM balancer_v2_ethereum.trades
  WHERE
    pool_id = 0x2d011adf89f0576c9b722c28269fcb5d50c2d17900020000000000000000024d
    AND token_sold_address = 0xf24d8651578a55b0c119b9910759a351a3458895
    AND block_time BETWEEN timestamp '{{Start Date}}' and timestamp '{{End Date}}'
)
SELECT
  block_time,
  AVG(price2) AS price2,
  AVG(price) AS price,
  SUM(amount) AS volume,
  SUM(buys) AS buys,
  SUM(sells) AS sells,
  CASE WHEN SUM(-1*(buys + sells)) >= 0 THEN 'buy' ELSE 'sell' END AS "volume_direction"
FROM (
    select *, token_sold_amount / token_bought_amount  as price2
    from sdBAL_buys
    
    UNION
    select *, token_bought_amount / token_sold_amount as price2
    from sdBAL_sells
)
GROUP BY
  "block_time"
ORDER BY
  "block_time" DESC