-- part of a query repo
-- query name: Balancer Volume by Pool (weekly top 5 except top pools) (Dune SQL)
-- query link: https://dune.com/queries/22274


-- Volume (pool breakdown) per week
-- Visualization: bar chart (stacked)

WITH swaps AS (
        SELECT
            date_trunc('week', d.block_time) AS week,
            version,
            sum(amount_usd) AS volume,
            CAST(d.project_contract_address as varchar) AS address,
            COUNT(DISTINCT tx_from) AS traders,
            blockchain
        FROM dex.trades d
        WHERE project = 'balancer'
        AND ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
        AND block_time >= TIMESTAMP'{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}'
        AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
         AND project_contract_address NOT IN (0x8b6e6e7b5b3801fed2cafd4b22b8a16c2f2db21a,
                                    0x1eff8af5d577060ba4ac8a29a13525bb0ee2a3d5,
                                    0x59a19d8c652fa0284f44113d0ff9aba70bd46fb4,
                                    0xc697051d1c6296c24ae3bcef39aca743861d9a81, 
                                    0x0b09dea16768f0799065c475be02919503cb2a3500020000000000000000001a)
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
        CONCAT('v', version, ': ', '(', SUBSTRING(s.blockchain,1,3),'), (', SUBSTRING(s.address, 3, 8), ')')) as pool,
        week,
        s.traders,
        ROW_NUMBER() OVER (PARTITION BY week ORDER BY SUM(volume) DESC NULLS LAST) AS position,
        ROUND(sum(s.volume), 2) AS volume
    FROM swaps s
    LEFT JOIN labels l ON s.address = CAST(l.address as varchar)
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4
) ranking
WHERE position <= 5
AND volume > 0