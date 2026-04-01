-- part of a query repo
-- query name: sdBAL swaps
-- query link: https://dune.com/queries/4005171


    SELECT
        block_time,
        token_bought_symbol,
        token_sold_symbol,
        token_bought_amount_raw,
        token_sold_amount_raw,
        tx_hash
    FROM balancer_v2_ethereum.trades
    WHERE
        pool_id = 0x2d011adf89f0576c9b722c28269fcb5d50c2d17900020000000000000000024d
        AND (token_bought_address = 0xf24d8651578a55b0c119b9910759a351a3458895
             OR token_sold_address = 0xf24d8651578a55b0c119b9910759a351a3458895)
    ORDER BY 1 DESC