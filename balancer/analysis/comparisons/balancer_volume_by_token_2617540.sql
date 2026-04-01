-- part of a query repo
-- query name: Balancer Volume by Token
-- query link: https://dune.com/queries/2617540


WITH swaps AS (
        SELECT 
            date_trunc('month', d.block_time) AS month,
            t.symbol AS token,
            SUM(amount_usd) AS volume
        FROM dex.trades d
        LEFT JOIN tokens.erc20 t ON d.token_bought_address = t.contract_address
        WHERE d.project = 'balancer'
        GROUP BY 1,2
),
    
    ranking AS (
        SELECT
            token, 
            ROW_NUMBER() OVER (ORDER BY SUM(volume) DESC NULLS LAST) AS position
        FROM swaps
        WHERE month = date_trunc('month', CURRENT_DATE - interval '1' month)
        GROUP BY 1
)


SELECT
    s.month, 
    CASE
        WHEN r.position <= 10 THEN s.token
        ELSE 'Others'
    END AS token, 
    SUM(s.volume) AS volume
FROM swaps s
LEFT JOIN ranking r ON r.token = s.token
GROUP BY 1, 2
ORDER BY 1, 2