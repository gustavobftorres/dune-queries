-- part of a query repo
-- query name: Gauge Bribes Metrics
-- query link: https://dune.com/queries/3976710


/* !Generated view warning: you can't query views in dune_user_generated anymore. All queries in DuneSQL are by default views though (try querying the table 'query_1747157') */
WITH
  week_period AS (
    SELECT DISTINCT
      t.round_id,
      t.start_date,
      t.end_date
    FROM
      balancer_ethereum.vebal_votes AS t
  ),
  gauge_vote_weekly AS (
    SELECT
      t.round_id,
      t.start_date,
      t.end_date,
      a.address,
      a.name,
      SUM(vote) AS vebal_votings,
      COUNT(DISTINCT provider) AS vote_users
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
      ) AS votes_weight,
      vote_users / CAST(
        SUM(vote_users) OVER (
          PARTITION BY
            start_date
        ) AS DOUBLE
      ) AS users_weight
    FROM
      gauge_vote_weekly
  ),
  bribe_deposit AS (
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
  bribe_value AS (
    SELECT
      t.*,
      t.amount / CAST(POWER(10, a.decimals) AS DOUBLE) AS amount,
      (t.amount * b.median_price) / CAST(POWER(10, a.decimals) AS DOUBLE) AS value
    FROM
      bribe_deposit AS t
      INNER JOIN tokens.erc20 AS a ON a.contract_address = t.token_addr
      INNER JOIN dex.prices AS b ON b.contract_address = a.contract_address
      AND b."hour" = DATE_TRUNC('hour', t."evt_block_time")
    WHERE
      a.blockchain = 'ethereum'
  ),
  bribe_value_week AS (
    SELECT
      round_id,
      "proposal",
      SUM(value) AS bribe_value
    FROM
      bribe_value AS t,
      week_period AS a
    WHERE
      t."evt_block_time" BETWEEN a.start_date AND a.end_date
    GROUP BY
      1,
      2
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
  )
SELECT
  CONCAT(
    CAST(
      COALESCE(
        CAST(
          COALESCE(TRY_CAST('Week' AS VARCHAR), '') AS VARCHAR
        ),
        ''
      ) AS VARCHAR
    ),
    CAST(
      COALESCE(
        CAST(
          COALESCE(TRY_CAST(t.round_id AS VARCHAR), '') AS VARCHAR
        ),
        ''
      ) AS VARCHAR
    )
  ) AS week,
  t.start_date,
  t.end_date,
  t.name,
  vote_users,
  CONCAT(
    CAST(
      COALESCE(
        CAST(
          COALESCE(
            TRY_CAST(
              ROUND(TRY_CAST(t.users_weight AS DECIMAL) * 100, 2) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        ''
      ) AS VARCHAR
    ),
    CAST(
      COALESCE(
        CAST(COALESCE(TRY_CAST('%' AS VARCHAR), '') AS VARCHAR),
        ''
      ) AS VARCHAR
    )
  ) AS users_weight,
  t.vebal_votings AS vebal_votes,
  CONCAT(
    CAST(
      COALESCE(
        CAST(
          COALESCE(
            TRY_CAST(
              ROUND(TRY_CAST(t.votes_weight AS DECIMAL) * 100, 2) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        ''
      ) AS VARCHAR
    ),
    CAST(
      COALESCE(
        CAST(COALESCE(TRY_CAST('%' AS VARCHAR), '') AS VARCHAR),
        ''
      ) AS VARCHAR
    )
  ) AS votes_weight,
  145000 * t.votes_weight AS bal_rewards,
  145000 * t.votes_weight * COALESCE(a.avg_price, today.avg_price) AS "bal_rewards($)",
  COALESCE(b.bribe_value, 0) AS "bribe_value($)",
  CASE
    WHEN b.bribe_value IS NULL THEN 0
    ELSE (
      145000 * t.votes_weight * COALESCE(a.avg_price, today.avg_price)
    ) / CAST(b.bribe_value AS DOUBLE)
  END AS "bribe_utility(bal rewards($)/bribe($))"
FROM
  gauge_weight AS t
  INNER JOIN bal_price_today AS today ON 1 = 1
  INNER JOIN bribe_value_week AS b ON b.proposal = t.address
  AND b.round_id = t.round_id
  LEFT JOIN bal_price AS a ON a.day = t.end_date
ORDER BY
  2 DESC,
  5 DESC