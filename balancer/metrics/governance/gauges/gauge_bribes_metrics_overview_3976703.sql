-- part of a query repo
-- query name: Gauge Bribes Metrics Overview
-- query link: https://dune.com/queries/3976703


/* !Generated view warning: you can't query views in dune_user_generated anymore. All queries in DuneSQL are by default views though (try querying the table 'query_1747157') */
/* total bribes gauge number */
WITH
  week_period AS (
    SELECT DISTINCT
      t.round_id,
      t.start_date,
      t.end_date
    FROM
      balancer_ethereum.vebal_votes AS t
  ),
  bribe_num AS (
    SELECT
      COUNT(DISTINCT "proposal") AS num
    FROM
      hiddenhand_ethereum.BalancerBribe_evt_DepositBribe AS t
  ),
  bribe_deposit /* total bribes value */ AS (
    SELECT
      t."evt_block_time",
      "proposal",
      CASE
        WHEN "token" = 0x9ddb2da7dd76612e0df237b89af2cf4413733212 THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        ELSE "token"
      END AS token_addr,
      /* token_addr */ t."amount",
      t."evt_tx_hash"
    FROM
      hiddenhand_ethereum.BalancerBribe_evt_DepositBribe AS t
  ),
  bribe_week AS (
    SELECT DISTINCT
      a."round_id",
      t."proposal"
    FROM
      bribe_deposit AS t,
      week_period AS a
    WHERE
      t."evt_block_time" >= a.start_date
      AND t."evt_block_time" < a.end_date
  ),
  bribe_value AS (
    SELECT
      SUM(
        (t.amount * b.median_price) / CAST(POWER(10, a.decimals) AS DOUBLE)
      ) AS value
    FROM
      bribe_deposit AS t
      INNER JOIN tokens.erc20 AS a ON a.contract_address = t.token_addr
      INNER JOIN dex.prices AS b ON b.contract_address = a.contract_address
      AND b."hour" = DATE_TRUNC('hour', t."evt_block_time")
    WHERE
      a.blockchain = 'ethereum'
  ),
  gauge_vote_weekly AS (
    SELECT
      t.round_id,
      t.start_date,
      t.end_date,
      address,
      name,
      SUM(vote) AS vebal_votings
    FROM
      balancer_ethereum.vebal_votes AS t
      LEFT JOIN labels.balancer_gauges AS a ON a.address = t.gauge
    GROUP BY
      1,
      2,
      3,
      4,
      5
  ),
  gauge_weight AS (
    SELECT
      *,
      vebal_votings / CAST(
        SUM(vebal_votings) OVER (
          PARTITION BY
            start_date
        ) AS DOUBLE
      ) AS votes_weight
    FROM
      gauge_vote_weekly
  ),
  bal_price AS (
    SELECT
      DATE_TRUNC('day', t."minute") AS day,
      AVG(t.price) AS avg_price
    FROM
      prices.usd AS t
      INNER JOIN tokens.erc20 AS a ON a.contract_address = t.contract_address
    WHERE
      a.blockchain = 'ethereum'
      AND t.blockchain = 'ethereum'
      AND a.symbol = 'BAL'
    GROUP BY
      1
  ),
  bal_price_today AS (
    SELECT
      AVG(t.price) AS avg_price
    FROM
      prices.usd AS t
      INNER JOIN tokens.erc20 AS a ON a.contract_address = t.contract_address
    WHERE
      a.blockchain = 'ethereum'
      AND t.blockchain = 'ethereum'
      AND a.symbol = 'BAL'
      AND DATE_TRUNC('day', t."minute") = DATE_TRUNC('day', CURRENT_TIMESTAMP)
  ),
  bribe_votes AS (
    SELECT
      SUM(vebal_votings) AS votes
    FROM
      bribe_week AS t
      INNER JOIN gauge_vote_weekly AS a ON a.address = t.proposal
      AND a.round_id = t.round_id
  ),
  bribe_users AS (
    SELECT
      COUNT(DISTINCT t.provider) AS users
    FROM
      balancer_ethereum.vebal_votes AS t
      INNER JOIN labels.balancer_gauges_arbitrum AS a ON a.address = t.gauge
      INNER JOIN bribe_week AS b ON b.proposal = a.address
      AND b.round_id = t.round_id
  ),
  bribe_rewards AS (
    SELECT
      SUM(
        145000 * t.votes_weight * COALESCE(a.avg_price, today.avg_price)
      ) AS rewards
    FROM
      gauge_weight AS t
      INNER JOIN bal_price_today AS today ON 1 = 1
      LEFT JOIN bal_price AS a ON a.day = t.end_date
  )
SELECT
  *
FROM
  bribe_num,
  bribe_value,
  bribe_votes,
  bribe_users,
  bribe_rewards