-- part of a query repo
-- query name: Parameterized Reclamm Markout
-- query link: https://dune.com/queries/5825146


WITH pool_tokens AS (
    SELECT 
        pool,
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[1]), '$.token'), 3)) as token_a, 
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[2]), '$.token'), 3)) as token_b 
    FROM balancer_v3_multichain.vault_evt_poolregistered where chain = '{{chain_pool}}' AND pool = {{pool}}
),
price_chain_A AS (
    SELECT (minute - interval '{{markout_minutes}}' minute) AS minute, CAST(price as DOUBLE) as price, symbol
    FROM prices.usd
    WHERE 
        blockchain = 'ethereum'
        and contract_address = {{token_a_address}}
        and minute > TIMESTAMP '{{start}}'
        and minute < TIMESTAMP '{{end}}'
),
price_chain_B AS (
    SELECT (minute - interval '{{markout_minutes}}' minute) AS minute, CAST(price as DOUBLE) as price, symbol
    FROM prices.usd
    WHERE 
        blockchain = 'ethereum'
        and contract_address = {{token_b_address}}
        and minute > TIMESTAMP '{{start}}'
        and minute < TIMESTAMP '{{end}}'
),
price_reclamm AS (
    SELECT 
        date_trunc('minute', T.block_time + interval '30' second) as "minute",
        CAST(CASE
            WHEN T.token_bought_address = PT.token_a THEN T.token_sold_amount / T.token_bought_amount
            ELSE T.token_bought_amount / T.token_sold_amount
        END AS DOUBLE) AS price,
        CASE
            -- Assuming token A is the most valuable token (randomly), when the token is leaving the pool, it's a negative markout
            WHEN T.token_bought_address = PT.token_a THEN -T.token_bought_amount
            -- Assuming token A is the most valuable token (randomly), when the token is entering the pool, it's a positive markout
            ELSE T.token_sold_amount
        END AS "amount_of_a_with_sign"
    FROM balancer_v3_multichain.vault_evt_swap S
    JOIN pool_tokens PT on PT.pool = {{pool}}
    JOIN dex.trades T ON 
        T.project = 'balancer' AND 
        T.blockchain = S.chain AND 
        T.block_time > TIMESTAMP '{{start}}' AND 
        T.block_time < TIMESTAMP '{{end}}' AND 
        S.evt_tx_hash = T.tx_hash AND 
        T.evt_index = S.evt_index
    WHERE S.pool = {{pool}} AND S.chain = '{{chain_pool}}' AND S.evt_block_time > TIMESTAMP '{{start}}' and S.evt_block_time < TIMESTAMP '{{end}}'
),
virtual_balance_events AS (
    SELECT DISTINCT 
        date_trunc('minute', evt_block_time + interval '30' second) as "minute",
        1 as is_rebalancing
    FROM balancer_v3_multichain.reclammpool_evt_virtualbalancesupdated
    WHERE chain = '{{chain_pool}}'
    AND contract_address = {{pool}}
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
        -- price is `amountB / amountA`, unit B/A
        -- `marketPrice - tradePrice` will give how was the difference of price in terms of B/A
        -- Multiply difference by `amountOfA`, and you have the absolute number of B tokens of difference between trade and market
        -- Multiply diffB by price of B in terms of USD, and you have how much USDC of difference
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