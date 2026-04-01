-- part of a query repo
-- query name: Aerodrome TVL
-- query link: https://dune.com/queries/3625769


WITH prices_usd AS (
  SELECT
    DATE_TRUNC('day', minute) AS day,
    contract_address,
    AVG(price) AS median_price
  FROM prices.usd
  WHERE
    blockchain = 'base'
  GROUP BY
    DATE_TRUNC('day', minute),
    contract_address
), 

dex_prices_1 AS (
  SELECT
    DATE_TRUNC('day', HOUR) AS DAY,
    contract_address AS token,
    APPROX_PERCENTILE(median_price, 0.5) AS price,
    SUM(sample_size) AS sample_size
  FROM dex.prices
  WHERE
    blockchain = 'base'
  GROUP BY
    DATE_TRUNC('day', HOUR),
    contract_address
  HAVING
    SUM(sample_size) > 3
), 

dex_prices_2 AS (
  SELECT
    day,
    token,
    price,
    LAG(price) OVER (PARTITION BY token ORDER BY day) AS previous_price
  FROM dex_prices_1
), 

dex_prices AS (
  SELECT
    day,
    token,
    price,
    LEAD(day, 1, CURRENT_TIMESTAMP) OVER (PARTITION BY token ORDER BY day) AS day_of_next_change
  FROM dex_prices_2
  WHERE
    (price < previous_price * 1e4 AND price > previous_price / 1e4)
), 

eth_prices AS (
    SELECT 
        DATE_TRUNC('day', minute) as day,
        AVG(price) as eth_price
    FROM prices.usd
    WHERE symbol = 'ETH'
    GROUP BY 1
),

pool_data AS (
  SELECT
    pool,
    token0,
    t1.decimals AS decimals0,
    t1.symbol AS symbol0,
    token1,
    t2.decimals AS decimals1,
    t2.symbol AS symbol1
  FROM aerodrome_base.PoolFactory_evt_PoolCreated AS p
  LEFT JOIN tokens.erc20 AS t1
    ON t1.contract_address = token0 AND t1.blockchain = 'base'
  LEFT JOIN tokens.erc20 AS t2
    ON t2.contract_address = token1 AND t2.blockchain = 'base'
), 

daily_balances AS (
  SELECT
    DATE_TRUNC('day', evt_block_time) AS day,
    d.contract_address,
    token0,
    symbol0,
    APPROX_PERCENTILE(reserve0, 0.5) / POWER(10, MAX(p.decimals0)) AS reserve0,
    token1,
    symbol1,
    APPROX_PERCENTILE(reserve1, 0.5) / POWER(10, MAX(p.decimals1)) AS reserve1
  FROM aerodrome_base.Pool_evt_Sync AS d
  INNER JOIN pool_data AS p
    ON d.contract_address = p.pool
  GROUP BY
    DATE_TRUNC('day', evt_block_time),
    d.contract_address,
    token0,
    symbol0,
    token1,
    symbol1
), 

daily_tvl AS (
  SELECT
    day,
    contract_address AS pool,
    token0 AS token,
    symbol0 AS symbol,
    reserve0 AS token_amount
  FROM daily_balances
  UNION ALL
  SELECT
    day,
    contract_address AS pool,
    token1 AS token,
    symbol1 AS symbol,
    reserve1 AS token_amount
  FROM daily_balances
)

SELECT
  t.day,
  t.pool,
  m.pool_type,
  t.token,
  t.symbol,
  AVG(t.token_amount * COALESCE(p.median_price, d.price)) AS liquidity_usd,
  AVG(t.token_amount * COALESCE(p.median_price, d.price) / eth_price) AS liquidity_eth
FROM daily_tvl AS t
LEFT JOIN prices_usd AS p
  ON t.token = p.contract_address AND t.day = p.day
LEFT JOIN dex_prices AS d
  ON t.token = d.token AND t.day >= d.day
LEFT JOIN eth_prices e ON e.day = t.day 
LEFT JOIN query_3629980 m ON t.pool = m.pool AND m.project = 'aerodrome'
WHERE UPPER(t.symbol) NOT IN ('LISA', 'RDMP') -- wrong price feeds --> outliers
GROUP BY t.day, t.pool, m.pool_type, t.token, t.symbol
HAVING AVG(t.token_amount * COALESCE(p.median_price, d.price)) < 1e9