-- part of a query repo
-- query name: BAL Accumulation Monitor — Holders Leaderboard (Weekly)
-- query link: https://dune.com/queries/6957887


-- ============================================================================
-- Q2: BAL Holder Leaderboard — Latest Snapshot with WoW + Total Delta
-- ----------------------------------------------------------------------------
-- One row per wallet showing the latest weekly balance, the change vs. the
-- previous week (WoW), and the change vs. the start of the tracking period.
--
-- Sort default: total_delta_since_start DESC (biggest accumulators on top)
--
-- Parameters: same as Q1 (start_date, min_bal, top_n, show_known)
-- Source:  erc20_ethereum.evt_transfer (BAL contract)
-- Labels:  labels.owner_addresses + manual overrides
-- ============================================================================

WITH params AS (
  SELECT
    0xba100000625a3754423978a60c9317c58a424e3d                AS bal_token,
    cast(date_trunc('week', date '2026-02-01') AS date)   AS start_week,
    cast(date_trunc('week', current_date)          AS date)   AS end_week
),

manual_labels AS (
  SELECT * FROM (VALUES
    (0xba12222222228d8ba445958a75a0704d566bf2c8, 'Balancer Vault',         'PROTOCOL'),
    (0xc128a9954e6c874ea3d62ce62b468ba073093f25, 'veBAL Contract',         'PROTOCOL'),
    (0x5c6ee304399dbdb9c8ef030ab642b10820db8f56, '80/20 BPT Pool',         'PROTOCOL'),
    (0xaf52695e1bb01a16d33d7194c28c42b10e0dbec2, 'Aura VoterProxy',        'PROTOCOL'),
    (0x10a19e7ee7d7f8a52822f6817de8ea18204f2e4f, 'Balancer DAO Multisig',  'TREASURY')
  ) AS t(addr, manual_label, manual_type)
),

flows AS (
  SELECT "to"   AS wallet,  cast(value as double) / 1e18 AS delta, evt_block_time
  FROM erc20_ethereum.evt_transfer, params
  WHERE contract_address = params.bal_token
  UNION ALL
  SELECT "from", -cast(value as double) / 1e18, evt_block_time
  FROM erc20_ethereum.evt_transfer, params
  WHERE contract_address = params.bal_token
),

-- Three balance snapshots in one pass:
--   1. balance at start_week        (everything BEFORE start_week)
--   2. balance one week ago         (everything BEFORE end_week)
--   3. balance now                  (everything up to and including end_week)
balances AS (
  SELECT
    f.wallet,
    sum(CASE WHEN f.evt_block_time < cast(p.start_week AS timestamp) THEN f.delta ELSE 0 END) AS bal_at_start,
    sum(CASE WHEN f.evt_block_time < cast(p.end_week   AS timestamp) THEN f.delta ELSE 0 END) AS bal_one_week_ago,
    sum(f.delta)                                                                              AS bal_now
  FROM flows f
  CROSS JOIN params p
  GROUP BY f.wallet
),

labelled AS (
  SELECT
    b.wallet,
    b.bal_now,
    b.bal_one_week_ago,
    b.bal_at_start,
    b.bal_now - b.bal_one_week_ago AS week_delta,
    b.bal_now - b.bal_at_start     AS total_delta,
    coalesce(
      ml.manual_label,
      nullif(lo.custody_owner, ''),
      nullif(lo.contract_name, ''),
      ''
    ) AS label,
    coalesce(
      ml.manual_type,
      CASE
        WHEN lower(lo.owner_key) IN (
          'coinbase','binance','kraken','kucoin','okx','bybit','htx','gateio','gate.io',
          'mexc','bitfinex','bitstamp','bitkub','bitso','indodax','uphold','revolut',
          'crypto.com','huobi','poloniex','bingx','bitget'
        ) THEN 'CEX'
        WHEN lower(lo.custody_owner) IN (
          'coinbase','binance','kraken','kucoin','okx','bybit','htx','gate.io','gateio',
          'mexc','bitfinex','bitstamp','bitkub','bitso','indodax','uphold','revolut',
          'crypto.com','huobi','poloniex','bingx','bitget'
        ) THEN 'CEX'
        WHEN lo.contract_name LIKE '%Safe%'                       THEN 'MULTISIG'
        WHEN lo.custody_owner IN ('balancer','aura_finance')      THEN 'PROTOCOL'
        WHEN lo.custody_owner IS NOT NULL AND lo.custody_owner <> '' THEN 'DEFI'
        ELSE 'EOA/OTHER'
      END
    ) AS entity_type
  FROM balances b
  LEFT JOIN manual_labels ml ON b.wallet = ml.addr
  LEFT JOIN labels.owner_addresses lo
    ON lo.address = b.wallet AND lo.blockchain = 'ethereum'
  WHERE b.bal_now >= 1000
    AND b.wallet <> 0x0000000000000000000000000000000000000000
)

SELECT
  wallet AS wallet_address,
  label,
  entity_type,
  bal_now                                AS balance_now,
  bal_one_week_ago                       AS balance_one_week_ago,
  week_delta,
  CASE
    WHEN bal_one_week_ago > 100 THEN (week_delta / bal_one_week_ago) * 100
    ELSE NULL
  END                                    AS week_pct_change,
  bal_at_start                           AS balance_at_period_start,
  total_delta,
  CASE
    WHEN bal_at_start > 100 THEN (total_delta / bal_at_start) * 100
    ELSE NULL
  END                                    AS total_pct_change,
  CASE
    WHEN bal_at_start <= 100 AND bal_now > 100 THEN 'NEW'
    WHEN bal_at_start  > 100 AND bal_now <= 100 THEN 'EXITED'
    WHEN total_delta >  100 THEN 'ACCUMULATING'
    WHEN total_delta < -100 THEN 'REDUCING'
    ELSE 'STABLE'
  END                                    AS movement
FROM labelled
WHERE
  'unlabeled_only' = 'all'
  OR ('unlabeled_only' = 'unlabeled_only' AND entity_type = 'EOA/OTHER')
ORDER BY total_delta DESC NULLS LAST
LIMIT 30;
