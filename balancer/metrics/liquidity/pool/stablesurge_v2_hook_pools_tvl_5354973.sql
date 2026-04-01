-- part of a query repo
-- query name: StableSurge v2 hook pools/TVL
-- query link: https://dune.com/queries/5354973


WITH
-- Get pools with the specific hooks contract
hook_pools AS (
  SELECT DISTINCT pool
  FROM balancer_v3_multichain.vault_evt_poolregistered
   WHERE hooksConfig LIKE '%"hooksContract":"0xbdbadc891bb95dee80ebc491699228ef0f7d6ff1"%'
     OR hooksConfig LIKE '%"hooksContract":"0x86705ee19c0509ff68f1118c55ee2ebde383d122"%'
     OR hooksConfig LIKE '%"hooksContract":"0x7c1b7a97bfacd39975de53e989a16c7bc4c78275"%'
     OR hooksConfig LIKE '%"hooksContract":"0xdb8d758bcb971e482b2c45f7f8a7740283a1bd3a"%'
     OR hooksConfig LIKE '%"hooksContract":"0x90bd26fbb9db17d75b56e4ca3a4c438fa7c93694"%'
     OR hooksConfig LIKE '%"hooksContract":"0xF39CA6ede9BF7820a952b52f3c94af526bAB9015"%'
),
-- Get the latest day available in balancer.liquidity
latest AS (
  SELECT
    MAX(day) AS latest_day
  FROM balancer.liquidity
),
-- Get token symbols for each pool
pool_tokens AS (
  SELECT 
    l.pool_address,
    l.blockchain,
    ARRAY_JOIN(ARRAY_AGG(DISTINCT l.token_symbol), '/') AS pool_name,
    SUM(l.pool_liquidity_usd) AS current_tvl_usd
  FROM balancer.liquidity l
  CROSS JOIN latest lt
  INNER JOIN hook_pools hp ON l.pool_address = hp.pool
  WHERE l.day = lt.latest_day
  GROUP BY l.pool_address, l.blockchain
)
SELECT
  pt.pool_address AS "Pool Address",
  pt.blockchain AS "Chain",
  COALESCE(pt.pool_name, 'Unknown') AS "Pool Name",
  COALESCE(pt.current_tvl_usd, 0) AS "TVL USD"
FROM pool_tokens pt
ORDER BY pt.current_tvl_usd DESC