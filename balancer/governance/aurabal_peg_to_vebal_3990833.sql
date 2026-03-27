-- part of a query repo
-- query name: auraBAL peg to veBAL
-- query link: https://dune.com/queries/3990833


WITH 
calendar AS (
        SELECT date_sequence AS day
        FROM unnest(sequence(date(TIMESTAMP '{{Start Date}}'), date(now()), interval '1' day)) as t(date_sequence)
), 

auraBAL_transactions AS (
    SELECT
        DATE_TRUNC('day', block_time) AS block_day,
        token_bought_symbol,
        token_sold_symbol,
        token_bought_amount_raw,
        token_sold_amount_raw,
        tx_hash
    FROM balancer_v2_ethereum.trades
    WHERE
        pool_id = 0x3dd0843a028c86e0b760b1a76929d1c5ef93a2dd000200000000000000000249
        AND (token_bought_address = 0x616e8bfa43f920657b3497dbf40d6b1a02d4608d
             OR token_sold_address = 0x616e8bfa43f920657b3497dbf40d6b1a02d4608d)
    AND block_time BETWEEN timestamp '{{Start Date}}' and timestamp '{{End Date}}'
),

exchange_rates AS (
    SELECT
        block_day,
        tx_hash,
        CASE
            WHEN token_bought_symbol = 'auraBAL' THEN CAST(token_sold_amount_raw AS DOUBLE) / token_bought_amount_raw
            ELSE CAST(token_bought_amount_raw AS DOUBLE) / token_sold_amount_raw
        END AS peg_rate,
        CASE
            WHEN token_bought_symbol = 'auraBAL' THEN token_bought_amount_raw + token_sold_amount_raw
            ELSE -(token_bought_amount_raw + token_sold_amount_raw)
        END AS net_volume
    FROM auraBAL_transactions
),


final AS(SELECT
    block_day,
    LEAD(block_day, 1, NOW()) OVER (ORDER BY block_day) AS day_of_next_change,
    SUM(peg_rate * ABS(net_volume)) / SUM(ABS(net_volume)) AS volume_weighted_peg,
    SUM(net_volume) AS total_volume,
    SUM(CASE WHEN net_volume > 0 THEN net_volume ELSE 0 END) AS total_buys,
    SUM(CASE WHEN net_volume < 0 THEN -net_volume ELSE 0 END) AS total_sells,
    CASE
        WHEN SUM(net_volume) > 0 THEN 'Net Buy'
        WHEN SUM(net_volume) < 0 THEN 'Net Sell'
        ELSE 'Balanced'
    END AS trade_direction
FROM exchange_rates
GROUP BY block_day
ORDER BY block_day DESC)

SELECT 
    c.day AS block_day,
    APPROX_PERCENTILE(volume_weighted_peg, 0.5) AS volume_weighted_peg
FROM calendar c
LEFT JOIN final a ON a.block_day <= c.day
AND c.day < a.day_of_next_change
GROUP BY 1