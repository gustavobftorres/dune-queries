-- part of a query repo
-- query name: Balancer Hourly Volume (Dune SQL)
-- query link: https://dune.com/queries/22257


WITH swaps AS (
        SELECT  
            date_trunc('hour', block_time) AS hour,
            version,
            COUNT(*) AS txns,
            SUM(amount_usd) AS volume
        FROM dex.trades
        WHERE project = 'balancer'
        AND ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
        AND block_time >= now() - interval '7' day
        AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
        GROUP BY 1, 2
    )

SELECT 
    hour, 
    version,
    txns AS "Swaps", 
    volume AS "Volume",
    volume/txns AS "Avg. Volume per Swap"
FROM swaps