-- part of a query repo
-- query name: Balancer Weekly LP Revenues - Token Breakdown (all-time top 10 revenues)
-- query link: https://dune.com/queries/63950


WITH swaps AS (
        SELECT
            date_trunc('week', d.block_time) AS week,
            d.token_b_address AS token_address,
            SUBSTRING(exchange_contract_address::text, 0, 43) AS pool_address,
            t.symbol AS token,
            swap_fee,
            sum(usd_amount)/2 AS volume
        FROM balancer.view_trades d
        LEFT JOIN erc20.tokens t ON t.contract_address = d.token_b_address
        WHERE ('{{Version}}' = 'Both' OR SUBSTRING('{{Version}}', 2) = version)
        GROUP BY 1,2,3,4,5
        
        UNION ALL
        
        SELECT
            date_trunc('week', d.block_time) AS week,
            d.token_a_address AS token_address,
            SUBSTRING(exchange_contract_address::text, 0, 43) AS pool_address,
            t.symbol AS token,
            swap_fee,
            sum(usd_amount)/2 AS volume
        FROM balancer.view_trades d
        LEFT JOIN erc20.tokens t ON t.contract_address = d.token_a_address
        WHERE ('{{Version}}' = 'Both' OR SUBSTRING('{{Version}}', 2) = version)
        GROUP BY 1,2,3,4,5
    ),

    token_revenues AS (
        SELECT
            week,
            s.token_address,
            token,
            SUM(volume*swap_fee) AS revenues
        FROM swaps s
        GROUP BY 1, 2, 3
    ),

    ranking AS (
        SELECT
            token_address,
            ROW_NUMBER() OVER (ORDER BY SUM(revenues) DESC NULLS LAST) AS position
        FROM token_revenues
        GROUP BY 1
)

SELECT
    week,
    t.token_address,
    COALESCE(token, t.token_address::text) AS token,
    revenues
FROM token_revenues t
LEFT JOIN ranking r ON t.token_address = r.token_address
WHERE position <= 10