-- part of a query repo
-- query name: User Metrics on Optimism
-- query link: https://dune.com/queries/3801068


WITH
    unique_addresses AS (
        SELECT
            block_date,
            'optimism' AS blockchain,
            COUNT(DISTINCT "from") AS unique_addresses
        FROM optimism.transactions
        WHERE to = 0xba12222222228d8ba445958a75a0704d566bf2c8
        AND block_time <= TIMESTAMP '{{End date}}'
        AND block_time >= TIMESTAMP '{{Start date}}'
        GROUP BY 1, 2
    )
SELECT
    block_date,
    blockchain,
    SUM(unique_addresses) OVER (PARTITION BY blockchain ORDER BY block_date) AS cumulative_unique_addresses,
    unique_addresses AS dau
FROM unique_addresses
ORDER BY block_date, blockchain;