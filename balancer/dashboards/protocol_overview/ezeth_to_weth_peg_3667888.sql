-- part of a query repo
-- query name: ezETH to WETH peg
-- query link: https://dune.com/queries/3667888


WITH ezeth_amounts AS(
    SELECT
        block_time,
        SUM(CASE WHEN token_bought_address = 0xbf5495efe5db9ce00f80364c8b423567e58d2110 THEN token_bought_amount
            WHEN token_sold_address = 0xbf5495efe5db9ce00f80364c8b423567e58d2110 THEN token_sold_amount
            ELSE 0 END) AS ezeth_volume
    FROM balancer.trades
    WHERE token_pair = 'ezETH-WETH'
    AND block_date >= TIMESTAMP '2024-04-24' - interval '1' day
    AND block_date <= TIMESTAMP '2024-04-24'
    AND blockchain = 'ethereum'
    GROUP BY 1
),

weth_amounts AS(
    SELECT
        block_time,
        SUM(CASE WHEN token_bought_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN token_bought_amount
            WHEN token_sold_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN token_sold_amount
            ELSE 0 END) AS weth_volume
    FROM balancer.trades
    WHERE token_pair = 'ezETH-WETH'
    AND block_date >= TIMESTAMP '2024-04-24' - interval '1' day
    AND block_date <= TIMESTAMP '2024-04-24'
    AND blockchain = 'ethereum'
    GROUP BY 1
)

SELECT
    e.block_time,
    e.ezeth_volume,
    w.weth_volume,
    w.weth_volume / e.ezeth_volume AS ezeth_to_weth_peg
FROM ezeth_amounts e
LEFT JOIN weth_amounts w ON e.block_time = w.block_time