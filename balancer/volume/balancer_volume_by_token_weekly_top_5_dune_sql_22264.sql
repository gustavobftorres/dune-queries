-- part of a query repo
-- query name: Balancer Volume By Token (weekly top 5) (Dune SQL)
-- query link: https://dune.com/queries/22264


WITH swaps AS (
        SELECT
            date_trunc('week', d.block_time) AS week,
            sum(amount_usd) AS volume,
            CAST(d.token_bought_address as varchar) AS address,
            t.symbol AS token
        FROM dex.trades d
        LEFT JOIN tokens.erc20 t ON CAST(t.contract_address as varchar) = CAST (d.token_bought_address as varchar)
        WHERE project = 'balancer'
        AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}'
        GROUP BY 1,3,4
        
        UNION ALL
        
        SELECT
            date_trunc('week', d.block_time) AS week,
            sum(amount_usd) AS volume,
            CAST(d.token_sold_address as varchar) AS address,
            t.symbol AS token
        FROM dex.trades d
        LEFT JOIN tokens.erc20 t ON CAST(t.contract_address as varchar) = CAST (d.token_bought_address as varchar)
        WHERE project = 'balancer'
        AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}'
        GROUP BY 1,3,4
)

SELECT * FROM (
        SELECT
        s.week,
        COALESCE(s.token, CONCAT(SUBSTRING(s.address, 3, 6), '...')) AS token,
        s.address,
        ROW_NUMBER() OVER (PARTITION BY week ORDER BY SUM(volume) DESC NULLS LAST) AS position,
        sum(s.volume)/2 AS volume
    FROM swaps s
    GROUP BY 1, 2, 3
    ORDER BY 1, 3
) ranking
WHERE position <= 5