-- part of a query repo
-- query name: Balancer V2 Weekly LP Revenues - Pool Breakdown (weekly top 5)
-- query link: https://dune.com/queries/71941


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
        s.address,
        COALESCE(CONCAT(SUBSTRING(UPPER(l.name), 1, 15), '(', SUBSTRING(CAST(s.address AS VARCHAR), 3, 8), ')'), CAST(s.address AS VARCHAR)) AS pool,
        ROW_NUMBER() OVER (PARTITION BY date_trunc('week', block_time) ORDER BY SUM(amount_usd*swap_fee) DESC NULLS LAST) AS position,
        SUM(amount_usd * swap_fee) AS revenues
    FROM swaps s
    LEFT JOIN labels l ON l.address = s.address
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
) ranking
WHERE position <= 5