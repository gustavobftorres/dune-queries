-- part of a query repo
-- query name: Swap Fee Revenues over Timeframes
-- query link: https://dune.com/queries/4083591


WITH swaps AS (
    SELECT 
        block_time,
        project_contract_address AS address,
        amount_usd,
        swap_fee
    FROM balancer.trades t
    WHERE 1 = 1
    AND ('{{1. Pool ID}}' = 'All' or CAST(project_contract_address  AS VARCHAR) = SUBSTRING('{{1. Pool ID}}', 1, 42))
    AND blockchain = '{{4. Blockchain}}'
    AND version = '2'
    AND block_time >= TIMESTAMP '{{2. Start date}}'
    AND block_time <= TIMESTAMP '{{3. End date}}'
    AND ('{{5. Token Address}}' = 'All' OR CAST(t.token_bought_address AS VARCHAR) = '{{5. Token Address}}')
),
revenues_by_period AS (
    SELECT 
        current_date - INTERVAL '7' day AS seven_days_ago,
        current_date - INTERVAL '30' day AS thirty_days_ago,
        current_date - INTERVAL '365' day AS one_year_ago,
        block_time,
        amount_usd * swap_fee AS revenue
    FROM swaps
)
SELECT
    SUM(CASE WHEN block_time >= seven_days_ago THEN revenue ELSE 0 END) AS revenue_7d,
    SUM(CASE WHEN block_time >= thirty_days_ago THEN revenue ELSE 0 END) AS revenue_30d,
    SUM(CASE WHEN block_time >= one_year_ago THEN revenue ELSE 0 END) AS revenue_1y,
    SUM(revenue) AS revenue_all_time
FROM revenues_by_period;
