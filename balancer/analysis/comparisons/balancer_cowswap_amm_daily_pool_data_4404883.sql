-- part of a query repo
-- query name: Balancer CoWSwap AMM Daily Pool Data
-- query link: https://dune.com/queries/4404883


WITH cow_trades AS(
  SELECT
      block_date,
      'ethereum' AS blockchain,
      trader AS project_contract_address,
      SUM(surplus_usd) AS surplus_usd
  FROM cow_protocol_ethereum.trades
  GROUP BY 1, 2, 3

UNION 

  SELECT
      block_date,
      'arbitrum' AS blockchain,
      trader AS project_contract_address,
      SUM(surplus_usd) AS surplus_usd
  FROM cow_protocol_arbitrum.trades
  GROUP BY 1, 2, 3

  UNION

  SELECT
      block_date,
      'gnosis' AS blockchain,
      trader AS project_contract_address,
      SUM(surplus_usd) AS surplus_usd
  FROM cow_protocol_gnosis.trades
  GROUP BY 1, 2, 3

UNION 

  SELECT
      block_date,
      'base' AS blockchain,
      trader AS project_contract_address,
      SUM(surplus_usd) AS surplus_usd
  FROM cow_protocol_base.trades
  GROUP BY 1, 2, 3
),

surplus AS (
  SELECT
    DATE_TRUNC('day', time) AS block_date,
    b.blockchain,
    cow_amm_address,
    COALESCE(surplus_usd, 0) AS surplus,
    SUM(protocol_fee_usd) AS protocol_fee_usd
  FROM dune.balancer.result_b_cow_amm_base_table b
  JOIN cow_trades t ON b.blockchain = t.blockchain 
  AND b.cow_amm_address = t.project_contract_address
  AND DATE_TRUNC('day', time) = t.block_date
  WHERE istrade
  GROUP BY 1, 2, 3, 4),

  trades AS(
    SELECT
        block_date,
        projecT_contract_address,
        blockchain,
        SUM(amount_usd) AS amount_usd
    FROM balancer_cowswap_amm.trades
    GROUP BY 1, 2, 3
  )
  
SELECT
    l.day,
    l.blockchain,
    l.pool_address,
    l.pool_symbol,
    t.amount_usd AS volume,
    SUM(l.protocol_liquidity_usd) AS tvl_usd,
    SUM(l.protocol_liquidity_eth) AS tvl_eth,
    c.surplus_usd AS surplus
FROM balancer_cowswap_amm.liquidity l
LEFT JOIN trades t
    ON l.blockchain = t.blockchain
    AND l.day = t.block_date
    AND l.pool_address = t.project_contract_address
LEFT JOIN cow_trades c
    ON l.blockchain = c.blockchain
    AND l.day = c.block_date
    AND l.pool_address = c.project_contract_address
LEFT JOIN surplus s
    ON l.blockchain = s.blockchain
    AND l.day = s.block_date
    AND l.pool_address = s.cow_amm_address
    AND s.surplus >= 0
WHERE l.day > TIMESTAMP '{{start date}}'
AND ('{{blockchain}}' = 'all' OR l.blockchain = '{{blockchain}}')
GROUP BY 1, 2, 3, 4, 5, 8
ORDER BY 1 DESC, 6 DESC