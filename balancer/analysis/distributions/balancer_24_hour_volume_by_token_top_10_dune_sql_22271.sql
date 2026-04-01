-- part of a query repo
-- query name: Balancer 24-hour Volume by Token (top 10) (Dune SQL)
-- query link: https://dune.com/queries/22271


-- Volume (token breakdown) per hour (last 24 hours)
-- Visualization: bar chart (stacked)

WITH swaps AS (
        SELECT
            date_trunc('hour', block_time) AS hour,
            sum(amount_usd) AS volume,
            CAST (d.token_bought_address as varchar) AS address,
            t.symbol AS token
        FROM dex.trades d
        LEFT JOIN tokens.erc20 t ON t.contract_address = d.token_bought_address
        WHERE project = 'balancer'
        AND date_trunc('hour', block_time) > date_trunc('hour', now() - interval '1' day)
        GROUP BY 1, 3, 4
        
        UNION ALL
        
        SELECT
            date_trunc('hour', block_time) AS hour,
           sum(amount_usd) AS volume,
           CAST(d.token_sold_address as varchar) AS address,
            t.symbol AS token
        FROM dex.trades d
        LEFT JOIN tokens.erc20 t ON t.contract_address = d.token_sold_address
        WHERE project = 'balancer'
        AND date_trunc('hour', block_time) > date_trunc('hour', now() - interval '1' day)
        GROUP BY 1, 3, 4
),

    ranking AS (
        SELECT
            token,
            address,
            sum(volume)/2,
            ROW_NUMBER() OVER (ORDER BY sum(volume) DESC NULLS LAST) AS position
        FROM swaps
        GROUP BY 1, 2
)

SELECT
    s.hour,
    sum(s.volume)/2 AS volume,
    s.address,
    COALESCE(s.token, CONCAT(SUBSTRING(s.address, 3, 6), '...')) AS token
FROM swaps s
LEFT JOIN ranking r ON r.address = s.address
WHERE r.position <= 10
GROUP BY 1,3,4
