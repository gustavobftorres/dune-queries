-- part of a query repo
-- query name: veBAL Wrappers Peg
-- query link: https://dune.com/queries/3994013


WITH 
calendar AS (
    SELECT date_sequence AS day
    FROM unnest(sequence(date(TIMESTAMP '{{Start Date}}'), date(now()), interval '1' day)) as t(date_sequence)
), 

aurabal_peg AS(
    WITH auraBAL_transactions AS (
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
    )
    SELECT
        block_day,
        'auraBAL' AS token,
        LEAD(block_day, 1, NOW()) OVER (ORDER BY block_day) AS day_of_next_change,
        SUM(peg_rate * ABS(net_volume)) / SUM(ABS(net_volume)) AS volume_weighted_peg
    FROM exchange_rates
    GROUP BY block_day, 2
    ORDER BY block_day DESC
),

tetubal_peg AS(
    WITH tetuBAL_transactions AS (
        SELECT
            DATE_TRUNC('day', block_time) AS block_day,
            token_bought_symbol,
            token_sold_symbol,
            token_bought_amount_raw,
            token_sold_amount_raw,
            tx_hash
        FROM balancer_v2_polygon.trades
        WHERE
            pool_id = 0x7af62c1ebf97034b7542ccec13a2e79bbcf34380000000000000000000000c13
            AND (token_bought_address = 0x7fc9e0aa043787bfad28e29632ada302c790ce33
                OR token_sold_address = 0x7fc9e0aa043787bfad28e29632ada302c790ce33)
            AND block_time BETWEEN timestamp '{{Start Date}}' and timestamp '{{End Date}}'
    ),
    exchange_rates AS (
        SELECT
            block_day,
            tx_hash,
            CASE
                WHEN token_bought_symbol = 'tetuBAL' THEN CAST(token_sold_amount_raw AS DOUBLE) / token_bought_amount_raw
                ELSE CAST(token_bought_amount_raw AS DOUBLE) / token_sold_amount_raw
            END AS peg_rate,
            CASE
                WHEN token_bought_symbol = 'tetuBAL' THEN token_bought_amount_raw + token_sold_amount_raw
                ELSE -(token_bought_amount_raw + token_sold_amount_raw)
            END AS net_volume
        FROM tetuBAL_transactions
    )
    SELECT
        block_day,
        'tetuBAL' AS token,
        LEAD(block_day, 1, NOW()) OVER (ORDER BY block_day) AS day_of_next_change,
        SUM(peg_rate * ABS(net_volume)) / SUM(ABS(net_volume)) AS volume_weighted_peg
    FROM exchange_rates
    GROUP BY block_day, 2
    ORDER BY block_day DESC
),

sdbal_peg AS(
    WITH sdBAL_transactions AS (
        SELECT
            DATE_TRUNC('day', block_time) AS block_day,
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
            AND block_time BETWEEN timestamp '{{Start Date}}' and timestamp '{{End Date}}'
    ),
    exchange_rates AS (
        SELECT
            block_day,
            tx_hash,
            CASE
                WHEN token_bought_symbol = 'sdBAL' THEN CAST(token_sold_amount_raw AS DOUBLE) / token_bought_amount_raw
                ELSE CAST(token_bought_amount_raw AS DOUBLE) / token_sold_amount_raw
            END AS peg_rate,
            CASE
                WHEN token_bought_symbol = 'sdBAL' THEN token_bought_amount_raw + token_sold_amount_raw
                ELSE -(token_bought_amount_raw + token_sold_amount_raw)
            END AS net_volume
        FROM sdBAL_transactions
        WHERE CASE
                WHEN token_bought_symbol = 'sdBAL' THEN token_bought_amount_raw > 100 * POWER(10,18)
                ELSE token_sold_amount_raw > 100 * POWER(10,18)
            END
    )
    SELECT
        block_day,
        'sdBAL' AS token,
        LEAD(block_day, 1, NOW()) OVER (ORDER BY block_day) AS day_of_next_change,
        SUM(peg_rate * ABS(net_volume)) / SUM(ABS(net_volume)) AS volume_weighted_peg
    FROM exchange_rates
    GROUP BY block_day, 2
    ORDER BY block_day DESC
),

all_pegs AS (
    SELECT * FROM aurabal_peg
    UNION ALL
    SELECT * FROM tetubal_peg
    UNION ALL 
    SELECT * FROM sdbal_peg
)

SELECT 
    c.day AS block_day,
    token,
    APPROX_PERCENTILE(volume_weighted_peg, 0.5) AS volume_weighted_peg
FROM calendar c
LEFT JOIN all_pegs a ON a.block_day <= c.day
AND c.day < a.day_of_next_change
WHERE volume_weighted_peg < 1
GROUP BY 1, 2
ORDER BY 1, 2;
