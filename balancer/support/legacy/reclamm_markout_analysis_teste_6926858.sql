-- part of a query repo
-- query name: reclamm_markout_analysis teste
-- query link: https://dune.com/queries/6926858


-- FINAL QUERY: reCLAMM Markout Analysis
-- Reads from materialized views for prices (no more scanning 416 GB prices.minute)
-- Same logic and minute granularity as the original query
--
WITH price_chain_A AS (
    SELECT "minute", price
    FROM query_6926838
),
price_chain_B AS (
    SELECT "minute", price
    FROM query_6926069
),
pool_tokens AS (
    SELECT
        pool,
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[1]), '$.token'), 3)) as token_a,
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[2]), '$.token'), 3)) as token_b
    FROM balancer_v3_plasma.vault_evt_poolregistered
    WHERE pool = 0xb3ca3ead1c59ded552cd30a6992038284b418b65
),
price_reclamm AS (
    SELECT
        date_trunc('minute', S.evt_block_time + interval '30' second) as "minute",
        CASE
            WHEN S.tokenIn = PT.token_a THEN CAST(S.amountOut as DOUBLE) * 1e12 / CAST(S.amountIn as DOUBLE)
            ELSE CAST(S.amountIn as DOUBLE) * 1e12 / CAST(S.amountOut as DOUBLE)
        END AS price,
        CASE
            WHEN S.tokenOut = PT.token_a THEN -CAST(S.amountOut AS DOUBLE)/1e18
            ELSE CAST(S.amountIn AS DOUBLE) / 1e18
        END AS "amount_of_a_with_sign"
    FROM balancer_v3_plasma.vault_evt_swap S
    JOIN pool_tokens PT on PT.pool = 0xb3ca3ead1c59ded552cd30a6992038284b418b65
    WHERE S.pool = 0xb3ca3ead1c59ded552cd30a6992038284b418b65
        AND S.evt_block_time > TIMESTAMP '2025-09-23 00:00:00'
        AND S.evt_block_time < now()
),
virtual_balance_events AS (
    SELECT DISTINCT
        date_trunc('minute', evt_block_time + interval '30' second) as "minute",
        1 as is_rebalancing
    FROM balancer_v3_plasma.reclammpool_evt_virtualbalancesupdated
    WHERE contract_address = 0xb3ca3ead1c59ded552cd30a6992038284b418b65
),
markout_each_trade AS (
    SELECT
        PR.minute,
        PR.price as trade_price,
        A.price/B.price as market_price,
        amount_of_a_with_sign,
        B.price as price_b_usdc,
        VBE.is_rebalancing
    FROM price_reclamm PR
    JOIN price_chain_A A ON A.minute = PR.minute
    JOIN price_chain_B B ON B.minute = PR.minute
    LEFT JOIN virtual_balance_events VBE ON VBE.minute = PR.minute
),
markout_pnl AS (
    SELECT
        minute,
        is_rebalancing,
        trade_price,
        market_price,
        amount_of_a_with_sign,
        (market_price - trade_price) * amount_of_a_with_sign * price_b_usdc as pnl_usdc
    FROM markout_each_trade
),
markout_sum AS (
    SELECT minute,
        is_rebalancing,
        trade_price,
        market_price,
        amount_of_a_with_sign,
        pnl_usdc,
        SUM(pnl_usdc) OVER (ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pnl_usdc
    FROM markout_pnl
),
markout_all_minutes AS (
    SELECT
        A.minute,
        MS.is_rebalancing,
        MS.trade_price,
        MS.market_price,
        MS.pnl_usdc,
        MS.cumulative_pnl_usdc
    FROM price_chain_A A
    LEFT JOIN markout_sum MS ON MS.minute = A.minute
),
markout_flagged AS (
    SELECT *,
        SUM(CASE WHEN trade_price IS NOT NULL THEN 1 ELSE 0 END)
            OVER (ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM markout_all_minutes
),
markout_filled AS (
    SELECT
        minute,
        MAX(is_rebalancing) OVER (PARTITION BY grp ORDER BY minute) AS is_rebalancing,
        MAX(cumulative_pnl_usdc) OVER (PARTITION BY grp ORDER BY minute) AS cumulative_pnl_usdc,
        MAX(trade_price) OVER (PARTITION BY grp ORDER BY minute) AS trade_price
    FROM markout_flagged
)
SELECT
    A."minute",
    A.price/B.price as "Market Price",
    MF.trade_price as "reCLAMM Price",
    MF.cumulative_pnl_usdc as "Markout USDC",
    CASE WHEN MF."is_rebalancing" = 1 THEN MF.cumulative_pnl_usdc ELSE 0 END AS "Rebalancing"
FROM price_chain_A AS A
JOIN price_chain_B AS B on A."minute" = B."minute"
LEFT JOIN markout_filled AS MF on A."minute" = MF."minute"
ORDER BY "minute"
