-- part of a query repo
-- query name: Balancer Weekly Volume (Dune SQL)
-- query link: https://dune.com/queries/22261


-- Volume per week
-- Visualization: bar chart

WITH swaps AS (
        SELECT  
            date_trunc('week', block_time) AS week,
            version,
            COUNT(*) AS txns,
            SUM(amount_usd) AS volume
        FROM dex.trades
        WHERE project = 'balancer'
        AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}'
        AND ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
        AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
        GROUP BY 1, 2
    )

SELECT 
    week, 
    version,
    txns AS "Swaps", 
    volume AS "Volume",
    volume/txns AS "Avg. Volume per Swap"
FROM swaps