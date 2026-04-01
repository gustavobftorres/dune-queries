-- part of a query repo
-- query name: Balancer Weekly LP Revenues - Token Breakdown (weekly top 5 volume)
-- query link: https://dune.com/queries/63936


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
    )

SELECT * FROM(
    SELECT
        week,
        s.token_address,
        COALESCE(token, s.token_address::text) AS token,
        SUM(volume * swap_fee) AS revenues,
        ROW_NUMBER() OVER (PARTITION BY week ORDER BY SUM(volume) DESC NULLS LAST) AS position
    FROM swaps s
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
    ) ranking
    WHERE position <= 5