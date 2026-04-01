-- part of a query repo
-- query name: tetuBAL B-80BAL-20WETH buys and sells
-- query link: https://dune.com/queries/3991133


WITH tetuBAL_buys AS (
  SELECT
    DATE_TRUNC('day', block_time) AS block_time,
    (
      CAST(token_bought_amount_raw as DOUBLE) / token_sold_amount_raw
    ) AS price,
    true as is_buying_tetu,
    token_bought_amount + token_sold_amount AS amount,
    tx_hash AS evt_tx_hash,
    token_bought_amount AS buys,
    -token_sold_amount AS sells,
    token_bought_amount,
    token_sold_amount
  FROM balancer_v2_polygon.trades
  WHERE
    pool_id = 0x7af62c1ebf97034b7542ccec13a2e79bbcf34380000000000000000000000c13
    AND token_bought_address = 0x7fc9e0aa043787bfad28e29632ada302c790ce33
    AND block_time BETWEEN timestamp '{{Start Date}}' and timestamp '{{End Date}}'
),
tetuBAL_sells AS (
  SELECT
    DATE_TRUNC('day', block_time) AS block_time,
    (
      CAST(token_bought_amount_raw as DOUBLE) / token_sold_amount_raw
    ) AS price,
    false as is_buying_tetu,
    token_bought_amount + token_sold_amount AS amount,
    tx_hash AS evt_tx_hash,
    token_bought_amount AS buys,
    -token_sold_amount AS sells,
    token_bought_amount,
    token_sold_amount
  FROM balancer_v2_polygon.trades
  WHERE
    pool_id = 0x7af62c1ebf97034b7542ccec13a2e79bbcf34380000000000000000000000c13
    AND token_sold_address = 0x7fc9e0aa043787bfad28e29632ada302c790ce33
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
    from tetuBAL_buys
    
    UNION
    select *, token_bought_amount / token_sold_amount as price2
    from tetuBAL_sells
)
GROUP BY
  "block_time"
ORDER BY
  "block_time" DESC