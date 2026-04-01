-- part of a query repo
-- query name: Balancer Unique Addresses on OP Superchain
-- query link: https://dune.com/queries/3771533


WITH
    unique_addresses AS (
        SELECT
            block_date,
            'base' AS blockchain,
            COUNT(DISTINCT "from") AS unique_addresses
        FROM base.transactions
        WHERE to = 0xba12222222228d8ba445958a75a0704d566bf2c8
        AND block_time <= TIMESTAMP '{{End date}}'
        AND block_time >= TIMESTAMP '{{Start date}}'
        GROUP BY 1,2
        
        UNION ALL
        
        SELECT
            block_date,
            'optimism' AS blockchain,
            COUNT(DISTINCT "from") AS unique_addresses
        FROM optimism.transactions
        WHERE to = 0xba12222222228d8ba445958a75a0704d566bf2c8
        AND block_time <= TIMESTAMP '{{End date}}'
        AND block_time >= TIMESTAMP '{{Start date}}'
        GROUP BY 1,2
    )
    
    SELECT
    block_date,
    blockchain,
    sum(unique_addresses) OVER (partition BY blockchain ORDER BY block_date) AS unique_addresses
    FROM unique_addresses