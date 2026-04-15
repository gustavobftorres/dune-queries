-- part of a query repo
-- query name: BAL Accumulation Monitor — Top Accumulators Weekly Snapshots
-- query link: https://dune.com/queries/6957888


-- ============================================================================
-- Q1: Weekly BAL Holder Snapshots — Top N Wallets, Long Format
-- ----------------------------------------------------------------------------
-- Goal: identify wallets accumulating BAL and chart their trajectory week by
-- week. Foundation query for line chart, pivot table, and leaderboard.
--
-- Output is one row per (wallet × week_starting), so it powers:
--   * Line chart   : x = week_starting, y = balance,        line = wallet
--   * Pivot table  : rows = wallet,    cols = week_starting, vals = balance
--   * Bar chart    : group = wallet,   stacks = entity_type
--
-- Selection rule: a wallet is included if its balance is >= 1000 in
-- the LATEST week. Then we pick the top 30 ranked by total accumulation
-- since 2026-02-01 (NEW entrants are ranked by their current balance).
--
-- Parameters:
--   start_date  - ISO date, snapshot start (default 2026-02-01)
--   min_bal     - minimum balance in latest week to be eligible (default 1000)
--   top_n       - max wallets returned (default 30)
--   show_known  - 'all' or 'unlabeled_only' (filter out CEXs/DeFi/protocols)
--
-- Source:  erc20_ethereum.evt_transfer (BAL contract)
-- Labels:  labels.owner_addresses + manual overrides
-- ============================================================================

WITH params AS (
  SELECT
    0xba100000625a3754423978a60c9317c58a424e3d                AS bal_token,
    -- Both start and end are aligned to Monday of the requested week.
    cast(date_trunc('week', date '2026-02-01') AS date)   AS start_week,
    cast(date_trunc('week', current_date)          AS date)   AS end_week
),

-- Hand-curated overrides for Balancer-ecosystem wallets that need clear labels.
manual_labels AS (
  SELECT * FROM (VALUES
    (0xba12222222228d8ba445958a75a0704d566bf2c8, 'Balancer Vault',         'PROTOCOL'),
    (0xc128a9954e6c874ea3d62ce62b468ba073093f25, 'veBAL Contract',         'PROTOCOL'),
    (0x5c6ee304399dbdb9c8ef030ab642b10820db8f56, '80/20 BPT Pool',         'PROTOCOL'),
    (0xaf52695e1bb01a16d33d7194c28c42b10e0dbec2, 'Aura VoterProxy',        'PROTOCOL'),
    (0x10a19e7ee7d7f8a52822f6817de8ea18204f2e4f, 'Balancer DAO Multisig',  'TREASURY')
  ) AS t(addr, manual_label, manual_type)
),

-- Generate one row per Monday-aligned week start, from start_week to end_week.
weeks AS (
  SELECT cast(week_start AS date) AS week_start
  FROM unnest(sequence(
    (SELECT start_week FROM params),
    (SELECT end_week FROM params),
    interval '7' day
  )) AS t(week_start)
),

-- Pull BAL transfers once and explode into (wallet, +/-amount, time) flows.
flows AS (
  SELECT "to"   AS wallet,  cast(value as double) / 1e18 AS delta, evt_block_time
  FROM erc20_ethereum.evt_transfer, params
  WHERE contract_address = params.bal_token
  UNION ALL
  SELECT "from", -cast(value as double) / 1e18, evt_block_time
  FROM erc20_ethereum.evt_transfer, params
  WHERE contract_address = params.bal_token
),

-- Starting balance: everything before the snapshot start day.
starting_balance AS (
  SELECT wallet, sum(delta) AS bal_at_start
  FROM flows, params
  WHERE evt_block_time < cast(params.start_week AS timestamp)
  GROUP BY wallet
),

-- Per-wallet weekly delta during the snapshot period.
weekly_delta AS (
  SELECT
    wallet,
    cast(date_trunc('week', evt_block_time) AS date) AS week_start,
    sum(delta)                                       AS week_delta
  FROM flows, params
  WHERE evt_block_time >= cast(params.start_week AS timestamp)
    AND evt_block_time <  cast(params.end_week + interval '7' day AS timestamp)
  GROUP BY wallet, cast(date_trunc('week', evt_block_time) AS date)
),

-- Every wallet that ever touched BAL during the period (or held some at start)
-- gets a row for every week — so we can compute running balance with a window.
all_wallets AS (
  SELECT wallet FROM starting_balance WHERE bal_at_start > 0
  UNION
  SELECT wallet FROM weekly_delta
),

scaffold AS (
  SELECT w.wallet, weeks.week_start
  FROM all_wallets w
  CROSS JOIN weeks
),

-- Running balance per wallet per week:
-- starting balance + cumulative sum of weekly deltas up to and including week_start.
running_balances AS (
  SELECT
    s.wallet,
    s.week_start,
    coalesce(sb.bal_at_start, 0) +
      sum(coalesce(wd.week_delta, 0)) OVER (
        PARTITION BY s.wallet
        ORDER BY s.week_start
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS balance
  FROM scaffold s
  LEFT JOIN starting_balance sb ON sb.wallet = s.wallet
  LEFT JOIN weekly_delta wd
    ON wd.wallet = s.wallet AND wd.week_start = s.week_start
),

-- Latest-week balance for each wallet (used for selecting top N).
latest_week AS (
  SELECT wallet, balance AS latest_balance
  FROM running_balances
  WHERE week_start = (SELECT end_week FROM params)
),

-- Total accumulation since start_day (latest balance - balance at start of period).
accumulation AS (
  SELECT
    lw.wallet,
    lw.latest_balance,
    coalesce(sb.bal_at_start, 0)                           AS bal_at_start,
    lw.latest_balance - coalesce(sb.bal_at_start, 0)       AS total_delta
  FROM latest_week lw
  LEFT JOIN starting_balance sb ON sb.wallet = lw.wallet
),

-- Apply labels & entity classification.
labelled AS (
  SELECT
    a.wallet,
    a.latest_balance,
    a.bal_at_start,
    a.total_delta,
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
  FROM accumulation a
  LEFT JOIN manual_labels ml ON a.wallet = ml.addr
  LEFT JOIN labels.owner_addresses lo
    ON lo.address = a.wallet AND lo.blockchain = 'ethereum'
  WHERE a.latest_balance >= 1000
    AND a.wallet <> 0x0000000000000000000000000000000000000000
),

-- Pick top N: prioritize by total_delta (accumulators), tiebreak on latest_balance.
top_wallets AS (
  SELECT *
  FROM labelled
  WHERE
    'unlabeled_only' = 'all'
    OR ('unlabeled_only' = 'unlabeled_only' AND entity_type = 'EOA/OTHER')
  ORDER BY total_delta DESC NULLS LAST, latest_balance DESC
  LIMIT 30
)

-- Final output: long format, one row per (wallet, week).
SELECT
  rb.wallet                                      AS wallet_address,
  tw.label                                       AS label,
  tw.entity_type                                 AS entity_type,
  rb.week_start                                    AS week_start,
  rb.balance                                     AS bal_balance,
  rb.balance - lag(rb.balance) OVER (
    PARTITION BY rb.wallet ORDER BY rb.week_start
  )                                              AS week_delta,
  tw.bal_at_start                                AS bal_at_period_start,
  tw.total_delta                                 AS total_delta_since_start,
  tw.latest_balance                              AS latest_balance,
  -- Display label that's always non-empty (falls back to short address)
  CASE
    WHEN tw.label <> '' THEN tw.label || ' (' || substr(cast(rb.wallet AS varchar), 1, 6) || '...' || substr(cast(rb.wallet AS varchar), 39, 4) || ')'
    ELSE substr(cast(rb.wallet AS varchar), 1, 6) || '...' || substr(cast(rb.wallet AS varchar), 39, 4)
  END                                            AS display_name
FROM running_balances rb
INNER JOIN top_wallets tw ON tw.wallet = rb.wallet
ORDER BY tw.total_delta DESC, rb.wallet, rb.week_start;
