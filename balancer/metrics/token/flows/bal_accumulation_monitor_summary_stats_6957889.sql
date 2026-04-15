-- part of a query repo
-- query name: BAL Accumulation Monitor — Summary Stats
-- query link: https://dune.com/queries/6957889


-- ============================================================================
-- Q3: BAL Holder Concentration & Movement Summary
-- ----------------------------------------------------------------------------
-- Single-row summary for the dashboard header counters:
--   * Number of holders now / on start_date
--   * Net new holders since start_date
--   * Top-N concentration share (10 / 50 / 100 wallets)
--   * Total BAL accumulated by EOA wallets (excluding CEX, DeFi, Protocol)
-- ============================================================================

WITH params AS (
  SELECT
    0xba100000625a3754423978a60c9317c58a424e3d   AS bal_token,
    timestamp '2026-02-01'                    AS start_day,
    date_trunc('day', cast(now() as timestamp))   AS end_day
),

manual_labels AS (
  SELECT * FROM (VALUES
    (0xba12222222228d8ba445958a75a0704d566bf2c8, 'PROTOCOL'),
    (0xc128a9954e6c874ea3d62ce62b468ba073093f25, 'PROTOCOL'),
    (0x5c6ee304399dbdb9c8ef030ab642b10820db8f56, 'PROTOCOL'),
    (0xaf52695e1bb01a16d33d7194c28c42b10e0dbec2, 'PROTOCOL')
  ) AS t(addr, manual_type)
),

transfers AS (
  SELECT "from" AS sender, "to" AS recipient,
         cast(value as double) / 1e18 AS amount, evt_block_time
  FROM erc20_ethereum.evt_transfer, params
  WHERE contract_address = params.bal_token
),

flows AS (
  SELECT recipient AS wallet, amount AS delta, evt_block_time FROM transfers
  UNION ALL
  SELECT sender,             -amount, evt_block_time FROM transfers
),

balances AS (
  SELECT
    wallet,
    sum(CASE WHEN evt_block_time < (SELECT start_day FROM params) THEN delta ELSE 0 END) AS bal_start,
    sum(delta) AS bal_now
  FROM flows
  WHERE wallet <> 0x0000000000000000000000000000000000000000
  GROUP BY wallet
),

labelled AS (
  SELECT
    b.wallet,
    b.bal_start,
    b.bal_now,
    b.bal_now - b.bal_start AS bal_delta,
    coalesce(
      ml.manual_type,
      CASE
        WHEN lower(lo.owner_key) IN (
          'coinbase','binance','kraken','kucoin','okx','bybit','htx','gateio','gate.io',
          'mexc','bitfinex','bitstamp','bitkub','bitso','indodax','uphold','revolut','crypto.com','huobi','poloniex'
        ) THEN 'CEX'
        WHEN lo.contract_name LIKE '%Safe%' THEN 'MULTISIG'
        WHEN lo.custody_owner IN ('balancer','aura_finance') THEN 'PROTOCOL'
        WHEN lo.custody_owner IS NOT NULL AND lo.custody_owner <> '' THEN 'DEFI'
        ELSE 'EOA/OTHER'
      END
    ) AS entity_type
  FROM balances b
  LEFT JOIN manual_labels ml ON b.wallet = ml.addr
  LEFT JOIN labels.owner_addresses lo
    ON lo.address = b.wallet AND lo.blockchain = 'ethereum'
),

ranked AS (
  SELECT
    wallet, bal_now, bal_start, bal_delta, entity_type,
    row_number() OVER (ORDER BY bal_now DESC) AS rank_now
  FROM labelled
  WHERE bal_now > 1
)

SELECT
  -- Holder counts
  count(*) FILTER (WHERE bal_now > 1)        AS holders_now,
  count(*) FILTER (WHERE bal_start > 1)      AS holders_start,
  count(*) FILTER (WHERE bal_now > 1) - count(*) FILTER (WHERE bal_start > 1) AS holders_net_change,
  count(*) FILTER (WHERE bal_start <= 1 AND bal_now > 1) AS new_holders_eoa_total,

  -- Total accumulation by EOA wallets only (excludes CEX, DeFi, Protocol)
  sum(CASE WHEN bal_delta > 0 AND entity_type = 'EOA/OTHER' THEN bal_delta ELSE 0 END)  AS bal_accumulated_eoa,
  sum(CASE WHEN bal_delta < 0 AND entity_type = 'EOA/OTHER' THEN -bal_delta ELSE 0 END) AS bal_distributed_eoa,
  sum(CASE WHEN entity_type = 'EOA/OTHER' THEN bal_delta ELSE 0 END) AS bal_net_eoa,

  -- Top-N concentration (share of all BAL held)
  (sum(bal_now) FILTER (WHERE rank_now <= 10)  / nullif(sum(bal_now), 0)) * 100 AS top10_share_pct,
  (sum(bal_now) FILTER (WHERE rank_now <= 50)  / nullif(sum(bal_now), 0)) * 100 AS top50_share_pct,
  (sum(bal_now) FILTER (WHERE rank_now <= 100) / nullif(sum(bal_now), 0)) * 100 AS top100_share_pct,

  sum(bal_now) AS total_bal_outstanding
FROM ranked;
