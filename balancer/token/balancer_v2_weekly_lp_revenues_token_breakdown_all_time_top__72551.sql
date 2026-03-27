-- part of a query repo
-- query name: Balancer V2 Weekly LP Revenues - Token Breakdown (all-time top 10)
-- query link: https://dune.com/queries/72551


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
            token_bought_symbol,
            swap_fee
        FROM balancer.trades t
        WHERE 1 = 1 AND blockchain = '{{4. Blockchain}}' AND version = '2'
        AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}'
    ),

ranking AS(
    SELECT
        token_bought_symbol,
        ROW_NUMBER() OVER (ORDER BY SUM(amount_usd*swap_fee) DESC NULLS LAST) AS position,
        SUM(amount_usd * swap_fee)/2 AS revenues
    FROM swaps s
    LEFT JOIN labels l ON l.address = s.address
    WHERE ('{{1. Pool ID}}' = 'All' or CAST(s.address AS VARCHAR) = SUBSTRING('{{1. Pool ID}}', 1, 42))
    GROUP BY 1
)

SELECT
    DATE_TRUNC('week', block_time) AS week,
    s.token_bought_symbol,
    SUM(amount_usd * swap_fee)/2 AS revenues
FROM swaps s
INNER JOIN ranking r ON s.token_bought_symbol = r.token_bought_symbol
AND r.position <= 10
GROUP BY 1, 2