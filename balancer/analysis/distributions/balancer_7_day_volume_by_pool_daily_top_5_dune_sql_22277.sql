-- part of a query repo
-- query name: Balancer 7-day Volume by Pool (daily top 5) (Dune SQL)
-- query link: https://dune.com/queries/22277


-- Volume (pool breakdown) per day
-- Visualization: bar chart (stacked)

WITH swaps AS (
        SELECT
            date_trunc('day', d.block_time) AS day,
            version,
            sum(amount_usd) AS volume,
            CAST(d.project_contract_address as varchar) AS address,
            COUNT(DISTINCT tx_from) AS traders,
            blockchain
        FROM dex.trades d
        WHERE project = 'balancer' AND ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
        AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
        AND date_trunc('day', d.block_time) > date_trunc('day', now() - interval '7' day)
        GROUP BY 1, 2, 4, 6
    ),

    labels AS (
    SELECT * FROM (SELECT
           address,
            name,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.addresses
        WHERE "category" IN ('balancer_pool', 'balancer_v2_pool', 'balancer_v3_pool')
        GROUP BY 1, 2) l
        WHERE num = 1
)

SELECT * FROM (
    SELECT
        s.address,
        COALESCE(CONCAT('v', version, ': ', '(', SUBSTRING(s.blockchain,1,3),') ',l.name, ' (', SUBSTRING(s.address, 3, 8), ')'),
        CONCAT('v', version, ': ', '(', SUBSTRING(s.blockchain,1,3),'), (', SUBSTRING(s.address, 3, 8), ')')) AS pool,
        day,
        s.traders,
        ROW_NUMBER() OVER (PARTITION BY day ORDER BY SUM(volume) DESC NULLS LAST) AS position,
        ROUND(sum(s.volume), 2) AS volume
    FROM swaps s
    LEFT JOIN labels l ON s.address = CAST(l.address as varchar)
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4
) ranking
WHERE position <= 5
AND volume > 0