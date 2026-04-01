-- part of a query repo
-- query name: Weekly Volume By Token, top 10 (Balancer Report)
-- query link: https://dune.com/queries/2945637


WITH swaps AS (
        SELECT 
        date_trunc('week', block_time) as "date",
        t.symbol AS token,
        SUM(amount_usd) AS volume
        FROM dex.trades d
        LEFT JOIN tokens.erc20 t ON d.token_bought_address = t.contract_address
        WHERE d.project = 'balancer' AND ('{{4. Blockchain}}' = 'All' OR d.blockchain = '{{4. Blockchain}}')
        AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}' 
        AND ('{{5. Version}}' = 'All' OR '{{5. Version}}' = version)
        GROUP BY 1,2
),
    
    ranking AS (
        SELECT
            token, 
            ROW_NUMBER() OVER (ORDER BY SUM(volume) DESC NULLS LAST) AS position
        FROM swaps
        WHERE "date" = date_trunc('week', CURRENT_DATE - interval '7' day)
        GROUP BY 1
)


SELECT
    s.date, 
    CASE
        WHEN r.position <= 10 THEN s.token
        ELSE 'Others'
    END AS token, 
    SUM(s.volume) AS volume
FROM swaps s
LEFT JOIN ranking r ON r.token = s.token
GROUP BY 1, 2
ORDER BY 1, 2