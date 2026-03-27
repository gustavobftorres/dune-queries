-- part of a query repo
-- query name: daily pool token supply change
-- query link: https://dune.com/queries/3749600


WITH
    daily_balance AS (
        SELECT
            block_date,
            blockchain,
            pool_type,
            pool_symbol,
            token_address,
            LEAD(block_date, 1, NOW()) OVER (PARTITION BY token_address ORDER BY block_date) AS day_of_next_change,
            SUM(delta_amount) AS daily_amount
        FROM query_3748761
        WHERE blockchain = 'arbitrum'
        GROUP BY 1, 2, 3, 4, 5
    ),

    calendar AS (
        SELECT date_sequence AS day
        FROM unnest(sequence(date('2021-04-21'), date(now()), interval '1' day)) as t(date_sequence)
    )
    
        SELECT
            c.day AS block_date,
            'arbitrum' as blockchain,
            '2' AS version,
            b.pool_type,
            b.pool_symbol,
            b.token_address,
            b.daily_amount
        FROM calendar c
        LEFT JOIN daily_balance b ON b.block_date = c.day
        WHERE b.token_address IS NOT NULL