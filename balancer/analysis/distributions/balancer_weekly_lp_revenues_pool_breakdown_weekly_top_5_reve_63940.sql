-- part of a query repo
-- query name: Balancer Weekly LP Revenues - Pool Breakdown (weekly top 5 revenues)
-- query link: https://dune.com/queries/63940


WITH labels AS (
        SELECT * FROM (SELECT
            address::text,
            name,
            ROW_NUMBER() OVER (PARTITION BY address::text ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.labels
        WHERE "type" IN ('balancer_pool', 'balancer_v2_pool')
        GROUP BY 1, 2) l
        WHERE num = 1
    ),
    
    swaps AS (
        SELECT 
            date_trunc('week', block_time) AS week,
            SUBSTRING(exchange_contract_address::text, 0, 43) AS address,
            version,
            swap_fee,
            usd_amount
        FROM balancer.view_trades
        WHERE ('{{Version}}' = 'Both' OR SUBSTRING('{{Version}}', 2) = version)
    )

SELECT * FROM (
    SELECT
        week,
        CONCAT('V', version) AS version,
        s.address,
        swap_fee,
        CONCAT(SUBSTRING(UPPER(l.name), 0, 15), ' (', 'V', version, ')', ' (', SUBSTRING(s.address, 3, 8), ')') AS pool,
        ROW_NUMBER() OVER (PARTITION BY week ORDER BY SUM(usd_amount * swap_fee) DESC NULLS LAST) AS position,
        SUM(usd_amount * swap_fee) AS revenues
    FROM swaps s
    LEFT JOIN labels l ON l.address = s.address
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 1, 2, 3, 4, 5
) ranking
WHERE position <= 5