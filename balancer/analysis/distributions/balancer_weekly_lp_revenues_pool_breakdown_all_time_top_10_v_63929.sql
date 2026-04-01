-- part of a query repo
-- query name: Balancer Weekly LP Revenues - Pool Breakdown (all-time top 10 volume)
-- query link: https://dune.com/queries/63929


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
            version,
            SUBSTRING(exchange_contract_address::text, 0, 43) AS address,
            usd_amount,
            swap_fee
        FROM balancer.view_trades
        WHERE ('{{Version}}' = 'Both' OR SUBSTRING('{{Version}}', 2) = version)
    ),
    
    ranking AS (
        SELECT
            address,
            ROW_NUMBER() OVER (ORDER BY SUM(usd_amount) DESC NULLS LAST) AS position
        FROM swaps
        GROUP BY 1
)

SELECT
    week,
    s.address,
    swap_fee,
    CONCAT('V', version) AS version,
    CONCAT(SUBSTRING(UPPER(l.name), 0, 15), ' (V', version, ')', ' (', SUBSTRING(s.address, 3, 8), ')') AS pool,
    SUM(usd_amount * swap_fee) AS revenues
FROM swaps s
LEFT JOIN labels l ON l.address = s.address
LEFT JOIN ranking r ON r.address = s.address
WHERE r.position <= 10
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5