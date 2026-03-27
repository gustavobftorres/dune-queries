-- part of a query repo
-- query name: Balancer V2 LP Daily Revenue (usd amounts based)
-- query link: https://dune.com/queries/72543


WITH
  labels AS (
    SELECT
      *
    FROM
      (
        SELECT
          address,
          name,
          ROW_NUMBER() OVER (
            PARTITION BY
              address
            ORDER BY
              MAX(updated_at) DESC
          ) AS num
        FROM
          labels.labels
        WHERE
          "type" = 'balancer_v2_pool'
        GROUP BY
          1,
          2
      ) AS l
    WHERE
      num = 1
  ),
  transfers AS (
    SELECT
      *
    FROM
      balancer_v2.view_transfers_bpt
    WHERE
      evt_block_time <= CAST('{{4. End date}}' AS TIMESTAMP)
  ),
  joins AS (
    SELECT
      DATE_TRUNC('day', e.evt_block_time) AS day,
      "to" AS lp,
      contract_address AS pool,
      SUM(value) / CAST(1e18 AS DOUBLE) AS amount
    FROM
      transfers AS e
    WHERE
      "from" IN (
        0xBA12222222228d8Ba445958a75a0704d566BF2C8,
        0x0000000000000000000000000000000000000000
      )
    GROUP BY
      1,
      2,
      3
  ),
  exits AS (
    SELECT
      DATE_TRUNC('day', e.evt_block_time) AS day,
      "from" AS lp,
      contract_address AS pool,
      - SUM(value) / CAST(1e18 AS DOUBLE) AS amount
    FROM
      transfers AS e
    WHERE
      "to" IN (
        0xBA12222222228d8Ba445958a75a0704d566BF2C8,
        0x0000000000000000000000000000000000000000
      )
    GROUP BY
      1,
      2,
      3
  ),
  daily_delta_bpt_by_pool AS (
    SELECT
      day,
      lp,
      pool,
      SUM(COALESCE(amount, 0)) AS amount
    FROM
      (
        SELECT
          *
        FROM
          joins AS j
        UNION ALL
        SELECT
          *
        FROM
          exits AS e
      ) AS foo
    WHERE
      (
        '{{2. Pool ID}}' = 'All'
        OR TRY_CAST(
          SUBSTRING(
            REGEXP_REPLACE('{{2. Pool ID}}', '^.', '\'),
            0,
            43
          ) AS VARBINARY
        ) = pool
      )
    GROUP BY
      1,
      2,
      3
  ),
  cumulative_bpt_by_pool AS (
    SELECT
      day,
      lp,
      pool,
      amount,
      LEAD(
        TRY_CAST(
          day AS TIMESTAMP
          WITH
            TIME ZONE
        ),
        1,
        TRY_CAST(
          CURRENT_DATE AS TIMESTAMP
          WITH
            TIME ZONE
        )
      ) OVER (
        PARTITION BY
          lp,
          pool
        ORDER BY
          day
      ) AS next_day,
      SUM(amount) OVER (
        PARTITION BY
          lp,
          pool
        ORDER BY
          day ROWS BETWEEN UNBOUNDED PRECEDING
          AND CURRENT ROW
      ) AS amount_bpt
    FROM
      daily_delta_bpt_by_pool
  ),
  calendar AS (
    SELECT
      day
    FROM
      cumulative_bpt_by_pool
      CROSS JOIN UNNEST (
        SEQUENCE(MIN(day), CURRENT_DATE, INTERVAL '1' day)
      ) AS _u (day)
  ),
  running_cumulative_bpt_by_pool AS (
    SELECT
      c.day,
      lp,
      pool,
      amount_bpt
    FROM
      cumulative_bpt_by_pool AS b
      JOIN calendar AS c ON b.day <= c.day
      AND c.day < b.next_day
  ),
  daily_total_bpt AS (
    SELECT
      day,
      pool,
      SUM(amount_bpt) AS total_bpt
    FROM
      running_cumulative_bpt_by_pool
    GROUP BY
      1,
      2
  ),
  lps_shares AS (
    SELECT
      c.day,
      c.lp,
      c.pool,
      c.amount_bpt / CAST(d.total_bpt AS DOUBLE) AS share
    FROM
      running_cumulative_bpt_by_pool AS c
      INNER JOIN daily_total_bpt AS d ON d.day = c.day
      AND d.pool = c.pool
    WHERE
      d.total_bpt > 0
  ),
  swaps AS (
    SELECT
      block_time,
      TRY_CAST(
        SUBSTRING(
          TRY_CAST(project_contract_address AS VARCHAR),
          0,
          43
        ) AS VARBINARY
      ) AS pool,
      amount_usd,
      COALESCE(
        s1."swapFeePercentage",
        s2."swapFeePercentage",
        s3."swapFeePercentage"
      ) / CAST(1e18 AS DOUBLE) AS swap_fee
    FROM
      dex.trades AS t
      LEFT JOIN balancer_v2_ethereum.WeightedPool_evt_SwapFeePercentageChanged AS s1 ON s1.contract_address = SUBSTRING(project_contract_address, 0, 21)
      AND s1.evt_block_time = (
        SELECT
          MAX(evt_block_time)
        FROM
          balancer_v2_ethereum.WeightedPool_evt_SwapFeePercentageChanged
        WHERE
          evt_block_time <= t.block_time
          AND contract_address = SUBSTRING(project_contract_address, 0, 21)
      )
      LEFT JOIN balancer_v2_ethereum.StablePool_evt_SwapFeePercentageChanged AS s2 ON s2.contract_address = SUBSTRING(project_contract_address, 0, 21)
      AND s2.evt_block_time = (
        SELECT
          MAX(evt_block_time)
        FROM
          balancer_v2_ethereum.StablePool_evt_SwapFeePercentageChanged
        WHERE
          evt_block_time <= t.block_time
          AND contract_address = SUBSTRING(project_contract_address, 0, 21)
      )
      LEFT JOIN balancer_v2_ethereum.LiquidityBootstrappingPool_evt_SwapFeePercentageChanged AS s3 ON s3.contract_address = SUBSTRING(project_contract_address, 0, 21)
      AND s3.evt_block_time = (
        SELECT
          MAX(evt_block_time)
        FROM
          balancer_v2_ethereum.LiquidityBootstrappingPool_evt_SwapFeePercentageChanged
        WHERE
          evt_block_time <= t.block_time
          AND contract_address = SUBSTRING(project_contract_address, 0, 21)
      )
    WHERE
      t.blockchain = 'ethereum'
      AND project = 'Balancer'
      AND block_time <= CAST('{{4. End date}}' AS TIMESTAMP)
      AND version = '2'
      AND (
        '{{2. Pool ID}}' = 'All'
        OR SUBSTRING(
          TRY_CAST(
            REGEXP_REPLACE('{{2. Pool ID}}', '^.', '\') AS VARBINARY
          ),
          0,
          21
        ) = SUBSTRING(project_contract_address, 0, 21)
      )
  ),
  revenues AS (
    SELECT
      DATE_TRUNC('day', block_time) AS day,
      pool,
      SUM(amount_usd) AS volume,
      SUM(amount_usd * swap_fee) AS revenues
    FROM
      swaps AS s
    GROUP BY
      1,
      2
  ),
  lp_revenues AS (
    SELECT
      s.day,
      s.pool,
      CONCAT(
        CAST(
          COALESCE(
            CAST(
              COALESCE(
                TRY_CAST(SUBSTRING(UPPER(name), 0, 15) AS VARCHAR),
                ''
              ) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        CAST(
          COALESCE(
            CAST(
              COALESCE(TRY_CAST(' (' AS VARCHAR), '') AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        CAST(
          COALESCE(
            CAST(
              COALESCE(TRY_CAST(SUBSTRING(s.pool, 0, 8) AS VARCHAR), '') AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        CAST(
          COALESCE(
            CAST(COALESCE(TRY_CAST(')' AS VARCHAR), '') AS VARCHAR),
            ''
          ) AS VARCHAR
        )
      ) AS label,
      lp,
      volume,
      (revenues * share) AS revenues
    FROM
      lps_shares AS s
      LEFT JOIN labels AS l ON l.address = s.pool
      LEFT JOIN revenues AS r ON r.day = s.day
      AND r.pool = s.pool
  ),
  cumulative_revenues AS (
    SELECT
      day,
      SUM(volume) AS volume,
      SUM(SUM(volume)) OVER (
        ORDER BY
          day
      ) AS cumulative_volume,
      SUM(revenues) AS revenues,
      SUM(SUM(revenues)) OVER (
        ORDER BY
          day
      ) AS cumulative_revenues
    FROM
      lp_revenues
    WHERE
      (
        '{{1. LP address}}' = 'All'
        OR TRY_CAST(
          REGEXP_REPLACE('{{1. LP address}}', '^.', '\') AS VARBINARY
        ) = lp
      )
    GROUP BY
      1
  )
SELECT
  *
FROM
  cumulative_revenues
WHERE
  day >= CAST('{{3. Start date}}' AS TIMESTAMP)
  AND day <= CAST('{{4. End date}}' AS TIMESTAMP)
  AND NOT cumulative_revenues IS NULL
ORDER BY
  day DESC