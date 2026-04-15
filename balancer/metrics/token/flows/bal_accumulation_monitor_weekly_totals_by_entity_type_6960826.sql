-- part of a query repo
-- query name: BAL Accumulation Monitor — Weekly Totals by Entity Type
-- query link: https://dune.com/queries/6960826


-- ============================================================================
-- Q3: Weekly BAL Totals by Entity Type — Stacked Area Chart Data
-- ----------------------------------------------------------------------------
-- For each week from 2026-02-01 onward, computes total BAL held by each
-- entity category. Powers a stacked area chart showing how BAL ownership is
-- distributed across CEXs, DeFi protocols, multisigs, the Balancer protocol
-- itself, and unknown EOAs.
--
-- Output is long format: one row per (week_start × entity_type).
--
-- Source: erc20_ethereum.evt_transfer (BAL contract)
-- Labels: labels.owner_addresses + manual overrides
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

starting_balance AS (
  SELECT wallet, sum(delta) AS bal_at_start
  FROM flows, params
  WHERE evt_block_time < cast(params.start_week AS timestamp)
  GROUP BY wallet
),

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

weeks AS (
  SELECT cast(week_start AS date) AS week_start
  FROM unnest(sequence(
    (SELECT start_week FROM params),
    (SELECT end_week FROM params),
    interval '7' day
  )) AS t(week_start)
),

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

classified AS (
  SELECT
    rb.week_start,
    rb.wallet,
    rb.balance,
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
  FROM running_balances rb
  LEFT JOIN manual_labels ml ON rb.wallet = ml.addr
  LEFT JOIN labels.owner_addresses lo
    ON lo.address = rb.wallet AND lo.blockchain = 'ethereum'
  WHERE rb.balance > 0
    AND rb.wallet <> 0x0000000000000000000000000000000000000000
)

SELECT
  week_start,
  entity_type,
  sum(balance)                  AS total_bal,
  count(*)                      AS wallet_count,
  sum(CASE WHEN balance >= 1000  THEN 1 ELSE 0 END) AS wallets_over_1k,
  sum(CASE WHEN balance >= 10000 THEN 1 ELSE 0 END) AS wallets_over_10k
FROM classified
GROUP BY week_start, entity_type
ORDER BY week_start, total_bal DESC;
