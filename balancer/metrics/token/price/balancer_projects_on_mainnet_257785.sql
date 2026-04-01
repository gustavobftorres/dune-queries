-- part of a query repo
-- query name: Balancer Projects on Mainnet
-- query link: https://dune.com/queries/257785


WITH projects AS (
        SELECT 
            name,
            address
        FROM labels.labels
        WHERE "type" = 'balancer_project'
        AND author IN ('balancerlabs', 'metacrypto', 'markusbkoch', 'mangool', 'astivelman')
    ),
    
    last_30d_volume AS (
        SELECT 
            p.name,
            SUM(usd_amount) AS last_30d_volume
        FROM dex.trades d
        JOIN projects p 
        ON p.address = SUBSTRING(exchange_contract_address, 0, 21)
        AND d.project = 'Balancer'
        AND block_time >= CURRENT_DATE - interval '30d'
        GROUP BY 1
    ),
    
    weekly_volume AS (
        SELECT 
            p.name,
            date_trunc('week', block_time) AS week,
            SUM(usd_amount) AS volume
        FROM dex.trades d
        JOIN projects p 
        ON p.address = SUBSTRING(exchange_contract_address, 0, 21)
        AND d.project = 'Balancer'
        AND block_time >= '{{1. Start date}}'
        AND block_time <= '{{2. End date}}'
        GROUP BY 1, 2
    ),
    
    volume_stats AS (
        SELECT
            name,
            SUM(volume) AS volume,
            AVG(volume) AS avg_volume
        FROM weekly_volume
        GROUP BY 1
    ),
    
    tvl_stats AS (
        SELECT p.name, SUM(liquidity) AS tvl
        FROM balancer.view_pools_liquidity l
        JOIN projects p
        ON p.address = SUBSTRING(l.pool, 0, 21)
        AND l.day = CURRENT_DATE
        GROUP BY 1
    )

SELECT v.name, tvl, volume, last_30d_volume, avg_volume
FROM volume_stats v
LEFT JOIN tvl_stats t
ON v.name = t.name
LEFT JOIN last_30d_volume l
ON v.name = l.name
