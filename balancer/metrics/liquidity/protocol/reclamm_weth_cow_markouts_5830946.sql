-- part of a query repo
-- query name: ReClamm WETH/COW Markouts
-- query link: https://dune.com/queries/5830946


-- mark-out ledger + cumulative PnL for an arbitrary N-minute horizon
-- add {{markout_minutes}} as a Number parameter in Dune

WITH trades AS (                               -- raw fills on the pool
    SELECT
        t.block_time,
        date_trunc(
           'minute',
           t.block_time + interval '{{markout_minutes}}' minute
        ) as "block_time_fut",
        t.tx_hash,
        t.evt_index,

        /* taker-signed WETH size (+ buy, – sell) */
        CASE
            WHEN t.token_bought_symbol = 'WETH' THEN  -t.token_sold_amount
            WHEN t.token_sold_symbol  = 'WETH' THEN t.token_bought_amount
        END                                   AS cow_signed_qty_taker,

        /* execution price (WETH per COW) */
        CASE
            WHEN t.token_bought_symbol = 'WETH'
                 THEN t.token_bought_amount / NULLIF(t.token_sold_amount, 0)
            WHEN t.token_sold_symbol  = 'WETH'
                 THEN t.token_sold_amount / NULLIF(t.token_bought_amount, 0)
        END                                   AS exec_price_eth_to_cow
    FROM balancer_v3_multichain.vault_evt_swap S
    JOIN dex.trades t ON 
        T.project = 'balancer' AND 
        T.blockchain = S.chain AND 
        T.block_time > TIMESTAMP '{{start}}' AND 
        T.block_time < TIMESTAMP '{{end}}' AND 
        S.evt_tx_hash = T.tx_hash AND 
        T.evt_index = S.evt_index
    WHERE S.pool = {{pool}} AND S.chain = '{{chain_pool}}' AND S.evt_block_time > TIMESTAMP '{{start}}' and S.evt_block_time < TIMESTAMP '{{end}}'
),

eth_prices AS (                               -- minute ETH-USD midpoint
    SELECT
        "minute" AS ts_minute,
        price
    FROM   prices.usd
    WHERE  blockchain = 'ethereum'
      AND "minute" > TIMESTAMP '{{start}}' 
      AND "minute" < TIMESTAMP '{{end}}'
      AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
),
cow_prices AS (                               -- minute COW-USD midpoint
    SELECT
        "minute" AS ts_minute,
        price
    FROM   prices.usd
    WHERE  blockchain = 'ethereum'
      AND "minute" > TIMESTAMP '{{start}}' 
      AND "minute" < TIMESTAMP '{{end}}'
      AND contract_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab
),
annotated AS (                                -- spot price at T + N minutes
    SELECT
        tr.*,
        -- WETH/COW = (USD/COW)/(USD/WETH)
        p_fut_cow.price/p_fut_eth.price AS price_markout
    FROM trades tr
    LEFT JOIN eth_prices p_fut_eth
           ON p_fut_eth.ts_minute = tr.block_time_fut
    LEFT JOIN cow_prices p_fut_cow
           ON p_fut_cow.ts_minute = tr.block_time_fut
),
markouts AS (                                 -- pool-side PnL in USDC
    SELECT
        block_time,
        tx_hash,
        evt_index,
        -cow_signed_qty_taker                     AS cow_signed_qty_pool,
        exec_price_eth_to_cow,
        price_markout,
        (price_markout - exec_price_eth_to_cow)
            * -cow_signed_qty_taker              AS pnl_eth
    FROM annotated
)

SELECT                                         -- final ledger
    *,
    SUM(pnl_eth) OVER (
        ORDER BY block_time, tx_hash, evt_index
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                          AS cumulative_pnl_eth
FROM   markouts
ORDER  BY block_time, tx_hash, evt_index       -- oldest → newest
