-- part of a query repo
-- query name: Reclamm Markout Transactions
-- query link: https://dune.com/queries/5825675


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
        T.tx_hash,
        T.evt_index,
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
markout_each_trade AS (
    SELECT 
        PR.minute, 
        PR.tx_hash,
        PR.evt_index,
        PR.price as trade_price, 
        -- market price of A in relation to B: (USD/tokenA)/(USD/tokenB) = tokenB/tokenA
        A.price/B.price as market_price, 
        amount_of_a_with_sign, 
        B.price as price_b_usdc
    FROM price_reclamm PR
    JOIN price_chain_A A ON A.minute = PR.minute
    JOIN price_chain_B B ON B.minute = PR.minute
)
SELECT 
    minute, 
    tx_hash,
    evt_index,
    trade_price, 
    market_price, 
    amount_of_a_with_sign, 
    -- price is `amountB / amountA`, unit B/A
    -- `marketPrice - tradePrice` will give how was the difference of price in terms of B/A
    -- Multiply difference by `amountOfA`, and you have the absolute number of B tokens of difference between trade and market (diffB)
    -- Multiply diffB by price of B in terms of USD, and you have how much USD of difference
    (market_price - trade_price) * amount_of_a_with_sign * price_b_usdc as pnl_usdc 
FROM markout_each_trade
ORDER BY minute