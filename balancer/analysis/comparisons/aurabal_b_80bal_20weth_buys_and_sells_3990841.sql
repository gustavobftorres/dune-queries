-- part of a query repo
-- query name: AuraBAL B-80BAL-20WETH buys and sells
-- query link: https://dune.com/queries/3990841


WITH auraBAL_buys AS (
  SELECT
    DATE_TRUNC('day', block_time) AS block_time,
    (
      CAST(token_bought_amount_raw as DOUBLE) / token_sold_amount_raw
    ) AS price,
    true as is_buying_aura,
    token_bought_amount + token_sold_amount AS amount,
    tx_hash AS evt_tx_hash,
    token_bought_amount AS buys,
    -token_sold_amount AS sells,
    token_bought_amount,
    token_sold_amount
  FROM balancer_v2_ethereum.trades
  WHERE
    pool_id = 0x3dd0843a028c86e0b760b1a76929d1c5ef93a2dd000200000000000000000249
    AND token_bought_address = 0x616e8bfa43f920657b3497dbf40d6b1a02d4608d
    AND block_time BETWEEN timestamp '{{Start Date}}' and timestamp '{{End Date}}'
),

auraBAL_sells AS (
  SELECT
    DATE_TRUNC('day', block_time) AS block_time,
    (
      CAST(token_bought_amount_raw as DOUBLE) / token_sold_amount_raw
    ) AS price,
    false as is_buying_aura,
    token_bought_amount + token_sold_amount AS amount,
    tx_hash AS evt_tx_hash,
    token_bought_amount AS buys,
    -token_sold_amount AS sells,
    token_bought_amount,
    token_sold_amount
  FROM balancer_v2_ethereum.trades
  WHERE
    pool_id = 0x3dd0843a028c86e0b760b1a76929d1c5ef93a2dd000200000000000000000249
    AND token_sold_address = 0x616e8bfa43f920657b3497dbf40d6b1a02d4608d
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
    from auraBAL_buys
    
    UNION
    select *, token_bought_amount / token_sold_amount as price2
    from auraBAL_sells
)
GROUP BY
  "block_time"
ORDER BY
  "block_time" DESC