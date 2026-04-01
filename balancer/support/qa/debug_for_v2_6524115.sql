-- part of a query repo
-- query name: Debug for V2
-- query link: https://dune.com/queries/6524115


WITH lbp_info AS (
    SELECT *
    FROM query_2511450
),

lbp_base AS (
    SELECT
        name AS lbp_name,
        blockchain,
        pool_id,
        from_hex(substr(CAST(pool_id AS varchar), 3)) AS pool_id_bytes,
        SUBSTRING(CAST(pool_id AS varchar), 1, 42) AS pool_address,
        start_time,
        end_time
    FROM lbp_info
),

weight_schedule AS (
    SELECT
        contract_address AS pool_address,
        startTime AS schedule_start_time,
        endTime   AS schedule_end_time,
        element_at(startWeights, 1) AS start_w0_raw,
        element_at(startWeights, 2) AS start_w1_raw,
        element_at(endWeights, 1)   AS end_w0_raw,
        element_at(endWeights, 2)   AS end_w1_raw
    FROM (
        SELECT
            contract_address,
            startTime,
            endTime,
            startWeights,
            endWeights,
            ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY evt_block_number DESC, evt_index DESC) AS rn
        FROM balancer_v2_ethereum.liquiditybootstrappingpool_evt_gradualweightupdatescheduled
    )
    WHERE rn = 1
),

token_meta AS (
    SELECT
        contract_address,
        symbol,
        decimals
    FROM tokens.erc20
    WHERE blockchain = 'ethereum'
),

trades AS (
    SELECT
        b.lbp_name,
        b.blockchain,
        b.pool_id,
        b.pool_id_bytes,
        b.pool_address,
        b.start_time,
        b.end_time,

        d.block_time,
        d.tx_hash,
        d.taker,
        d.amount_usd,
        d.token_pair,

        d.token_sold_address,
        d.token_bought_address,
        d.token_sold_amount_raw,
        d.token_bought_amount_raw
    FROM dex.trades d
    JOIN lbp_base b
      ON b.pool_address = CAST(d.project_contract_address AS varchar)
     AND b.blockchain = d.blockchain
    WHERE d.project = 'balancer'
      AND d.block_time <= b.end_time
      AND (b.start_time IS NULL OR d.block_time >= b.start_time)
),

agg AS (
    SELECT
        t.lbp_name,
        t.blockchain,
        t.pool_id,
        t.pool_id_bytes,
        t.pool_address,
        MIN(t.start_time) AS start_time,
        MAX(t.end_time)   AS end_time,

        COUNT(DISTINCT t.tx_hash) AS txns,
        COUNT(DISTINCT t.taker)   AS participants,
        COUNT(*) AS total_swaps,

        COALESCE(SUM(t.amount_usd), 0) AS volume_usd,
        array_agg(DISTINCT t.token_pair) AS token_pairs
    FROM trades t
    GROUP BY 1,2,3,4,5
),

pair_volume AS (
    SELECT
        a.pool_id_bytes,
        d.token_pair,
        SUM(d.amount_usd) AS volume_usd
    FROM agg a
    JOIN dex.trades d
      ON d.blockchain = 'ethereum'
     AND d.project = 'balancer'
     AND CAST(d.project_contract_address AS varchar) = a.pool_address
     AND d.block_time BETWEEN a.start_time AND a.end_time
    GROUP BY 1,2
),

primary_pair AS (
    SELECT
        x.pool_id_bytes,
        x.token_pair
    FROM (
        SELECT
            pv.pool_id_bytes,
            pv.token_pair,
            pv.volume_usd,
            ROW_NUMBER() OVER (PARTITION BY pv.pool_id_bytes ORDER BY pv.volume_usd DESC) AS rn
        FROM pair_volume pv
    ) x
    WHERE x.rn = 1
),

lbp_window AS (
    SELECT
        a.pool_id_bytes,
        a.pool_address,
        a.start_time,
        a.end_time,

        from_unixtime(CAST(ws.schedule_start_time AS double)) AS schedule_start_ts,
        from_unixtime(CAST(ws.schedule_end_time   AS double)) AS schedule_end_ts,

        COALESCE(a.start_time, from_unixtime(CAST(ws.schedule_start_time AS double))) AS window_start,
        COALESCE(a.end_time,   from_unixtime(CAST(ws.schedule_end_time   AS double))) AS window_end
    FROM agg a
    LEFT JOIN weight_schedule ws
      ON ws.pool_address = from_hex(substr(a.pool_address, 3))
),

-- ✅ Inferir project/payment tokens pelas trades (robusto e não depende de TokensRegistered)
project_token_from_trades AS (
    SELECT
        w.pool_id_bytes,
        token_bought_address AS project_token,
        SUM(amount_usd) AS bought_usd
    FROM lbp_window w
    JOIN dex.trades d
      ON d.blockchain='ethereum'
     AND d.project='balancer'
     AND CAST(d.project_contract_address AS varchar) = w.pool_address
     AND d.block_time BETWEEN w.window_start AND w.window_end
     AND d.amount_usd IS NOT NULL
    GROUP BY 1,2
),

payment_token_from_trades AS (
    SELECT
        w.pool_id_bytes,
        token_sold_address AS payment_token,
        SUM(amount_usd) AS sold_usd
    FROM lbp_window w
    JOIN dex.trades d
      ON d.blockchain='ethereum'
     AND d.project='balancer'
     AND CAST(d.project_contract_address AS varchar) = w.pool_address
     AND d.block_time BETWEEN w.window_start AND w.window_end
     AND d.amount_usd IS NOT NULL
    GROUP BY 1,2
),

token_side AS (
    SELECT
        w.pool_id_bytes,

        -- top bought = project token
        p.project_token,
        -- top sold = payment token
        s.payment_token,

        -- pesos (normalizados)
        (CAST(ws.start_w0_raw AS double) / 1e18) AS start_w0,
        (CAST(ws.end_w0_raw   AS double) / 1e18) AS end_w0,
        (CAST(ws.start_w1_raw AS double) / 1e18) AS start_w1,
        (CAST(ws.end_w1_raw   AS double) / 1e18) AS end_w1

    FROM lbp_window w
    JOIN weight_schedule ws
      ON ws.pool_address = from_hex(substr(w.pool_address, 3))

    JOIN (
        SELECT pool_id_bytes, project_token
        FROM (
            SELECT
                pool_id_bytes,
                project_token,
                bought_usd,
                ROW_NUMBER() OVER (PARTITION BY pool_id_bytes ORDER BY bought_usd DESC) AS rn
            FROM project_token_from_trades
        ) x
        WHERE rn = 1
    ) p ON p.pool_id_bytes = w.pool_id_bytes

    JOIN (
        SELECT pool_id_bytes, payment_token
        FROM (
            SELECT
                pool_id_bytes,
                payment_token,
                sold_usd,
                ROW_NUMBER() OVER (PARTITION BY pool_id_bytes ORDER BY sold_usd DESC) AS rn
            FROM payment_token_from_trades
        ) x
        WHERE rn = 1
    ) s ON s.pool_id_bytes = w.pool_id_bytes
),

-- ✅ Initial price no começo do LBP (1h)
initial_price AS (
    SELECT
        w.pool_id_bytes,
        MIN(d.block_time) AS initial_price_time,
        min_by(
          d.amount_usd
          / NULLIF(CAST(d.token_bought_amount AS double), 0),
          d.block_time
        ) AS initial_price_usd
    FROM lbp_window w
    JOIN token_side ts
      ON ts.pool_id_bytes = w.pool_id_bytes
    JOIN dex.trades d
      ON d.blockchain='ethereum'
     AND d.project='balancer'
     AND CAST(d.project_contract_address AS varchar) = w.pool_address
     AND d.block_time >= w.window_start
     AND d.block_time <  w.window_start + interval '1' hour
     AND d.token_bought_address = ts.project_token
     AND d.token_sold_address   = ts.payment_token
     AND d.amount_usd IS NOT NULL
     AND CAST(d.token_bought_amount AS double) > 0
    GROUP BY 1
),

drawdown_trades AS (
    SELECT
        w.pool_id_bytes,

        LEAST(
          1.0,
          GREATEST(
            0.0,
            date_diff('second', w.window_start, d.block_time)
            / NULLIF(date_diff('second', w.window_start, w.window_end), 0)
          )
        ) AS t_norm,

        -- trade price (payment per project) com decimals
        (CAST(d.token_sold_amount_raw AS double) / power(10, COALESCE(tm_sold.decimals, 18)))
        /
        NULLIF((CAST(d.token_bought_amount_raw AS double) / power(10, COALESCE(tm_buy.decimals, 18))), 0) AS trade_price,

        d.amount_usd,

        ip.initial_price_usd,

        ts.start_w0, ts.end_w0, ts.start_w1, ts.end_w1,

        ts.project_token,
        ts.payment_token

    FROM lbp_window w
    JOIN token_side ts
      ON ts.pool_id_bytes = w.pool_id_bytes
    JOIN initial_price ip
      ON ip.pool_id_bytes = w.pool_id_bytes

    JOIN dex.trades d
      ON d.blockchain='ethereum'
     AND d.project='balancer'
     AND CAST(d.project_contract_address AS varchar) = w.pool_address
     AND d.block_time BETWEEN w.window_start AND w.window_end
     AND d.token_bought_address = ts.project_token
     AND d.token_sold_address   = ts.payment_token
     AND CAST(d.token_bought_amount_raw AS double) > 0
     AND CAST(d.token_sold_amount_raw AS double) > 0
     AND d.amount_usd IS NOT NULL

    LEFT JOIN token_meta tm_sold
      ON tm_sold.contract_address = d.token_sold_address
    LEFT JOIN token_meta tm_buy
      ON tm_buy.contract_address = d.token_bought_address
),

priced_trades AS (
    SELECT
        dt.pool_id_bytes,
        dt.trade_price,
        dt.amount_usd,

        -- ✅ escolher qual peso é project: o que diminui (start > end)
        CASE
          WHEN dt.start_w0 > dt.end_w0
            THEN (dt.start_w0 + (dt.end_w0 - dt.start_w0) * dt.t_norm)  -- project weight
          ELSE (dt.start_w1 + (dt.end_w1 - dt.start_w1) * dt.t_norm)
        END AS w_proj_t,

        CASE
          WHEN dt.start_w0 > dt.end_w0
            THEN (dt.start_w1 + (dt.end_w1 - dt.start_w1) * dt.t_norm)  -- payment weight
          ELSE (dt.start_w0 + (dt.end_w0 - dt.start_w0) * dt.t_norm)
        END AS w_pay_t,

        CASE WHEN dt.start_w0 > dt.end_w0 THEN dt.start_w0 ELSE dt.start_w1 END AS w_proj_0,
        CASE WHEN dt.start_w0 > dt.end_w0 THEN dt.start_w1 ELSE dt.start_w0 END AS w_pay_0,

        dt.initial_price_usd,

        -- programmed price ancorado em initial_price_usd e no ratio (w_proj/w_pay)
        dt.initial_price_usd
        * (
            ( (CASE WHEN dt.start_w0 > dt.end_w0
                     THEN (dt.start_w0 + (dt.end_w0 - dt.start_w0) * dt.t_norm)
                     ELSE (dt.start_w1 + (dt.end_w1 - dt.start_w1) * dt.t_norm)
                END)
              / NULLIF(
                  (CASE WHEN dt.start_w0 > dt.end_w0
                        THEN (dt.start_w1 + (dt.end_w1 - dt.start_w1) * dt.t_norm)
                        ELSE (dt.start_w0 + (dt.end_w0 - dt.start_w0) * dt.t_norm)
                   END),
                  0
                )
            )
            /
            NULLIF(
              ( (CASE WHEN dt.start_w0 > dt.end_w0 THEN dt.start_w0 ELSE dt.start_w1 END)
                / NULLIF((CASE WHEN dt.start_w0 > dt.end_w0 THEN dt.start_w1 ELSE dt.start_w0 END), 0)
              ),
              0
            )
          ) AS programmed_price
    FROM drawdown_trades dt
),

max_drawdown AS (
    SELECT
        pool_id_bytes,
        MAX(
          CASE
            WHEN programmed_price > 0 AND trade_price < programmed_price
              THEN (programmed_price - trade_price) / programmed_price
            ELSE 0.0
          END
        ) AS max_drawdown
    FROM priced_trades
    GROUP BY 1
),

buy_pressure AS (
    SELECT
        pool_id_bytes,
        SUM(
          CASE
            WHEN programmed_price > 0
              THEN GREATEST(0.0, (trade_price / programmed_price) - 1.0) * amount_usd
            ELSE 0.0
          END
        )
        / NULLIF(
            SUM(CASE WHEN programmed_price > 0 THEN amount_usd ELSE 0.0 END),
            0.0
          ) AS buy_pressure_avg
    FROM priced_trades
    GROUP BY 1
),

drawdown_debug AS (
    SELECT pool_id_bytes, COUNT(*) AS n_drawdown_trades
    FROM drawdown_trades
    GROUP BY 1
)

SELECT
    a.lbp_name,
    a.blockchain,
    a.pool_id,
    a.pool_address,

    w.window_start AS start_time,
    w.window_end   AS end_time,

    date_diff('second', w.window_start, w.window_end) AS duration_seconds,
    (date_diff('second', w.window_start, w.window_end) / 86400.0) AS duration_days,
    (date_diff('second', w.window_start, w.window_end) / 3600.0) AS duration_hours,

    a.txns,
    a.participants,
    a.total_swaps,

    (a.total_swaps / NULLIF((date_diff('second', w.window_start, w.window_end) / 3600.0), 0)) AS swaps_per_hour,

    a.volume_usd,
    pp.token_pair AS primary_token_pair,
    a.token_pairs,

    ip.initial_price_time,
    ip.initial_price_usd,

    dd.n_drawdown_trades,
    md.max_drawdown,
    bp.buy_pressure_avg

FROM agg a
LEFT JOIN lbp_window w
  ON w.pool_id_bytes = a.pool_id_bytes
LEFT JOIN primary_pair pp
  ON pp.pool_id_bytes = a.pool_id_bytes
LEFT JOIN initial_price ip
  ON ip.pool_id_bytes = a.pool_id_bytes
LEFT JOIN drawdown_debug dd
  ON dd.pool_id_bytes = a.pool_id_bytes
LEFT JOIN max_drawdown md
  ON md.pool_id_bytes = a.pool_id_bytes
LEFT JOIN buy_pressure bp
  ON bp.pool_id_bytes = a.pool_id_bytes

ORDER BY a.volume_usd DESC;
