-- part of a query repo
-- query name: Weekly Swap Fee Revenues
-- query link: https://dune.com/queries/2950416


WITH labels AS (
    SELECT * FROM (SELECT
            address,
            name,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.addresses
        WHERE "category" = 'balancer_v2_pool'
        GROUP BY 1, 2) l
        WHERE num = 1
    ),
    
    swaps AS (
        SELECT 
            block_time,
            project_contract_address AS address,
            amount_usd,
            swap_fee
        FROM balancer.trades t
        WHERE 1 = 1 AND blockchain = '{{4. Blockchain}}' AND version = '2'
        AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}'
        AND ('{{5. Token Address}}' = 'All' or CAST(t.token_bought_address as VARCHAR) = '{{5. Token Address}}')
    )

SELECT * FROM (
    SELECT
        date_trunc('week', block_time) AS week,
        SUM(amount_usd * swap_fee) AS revenues
    FROM swaps s
    LEFT JOIN labels l ON l.address = s.address
    WHERE ('{{1. Pool ID}}' = 'All' or CAST(s.address AS VARCHAR) = SUBSTRING('{{1. Pool ID}}', 1, 42))
    GROUP BY 1
    ORDER BY 1
) ranking