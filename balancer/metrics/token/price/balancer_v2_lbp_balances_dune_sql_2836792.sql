-- part of a query repo
-- query name: Balancer V2 LBP Balances (Dune SQL)
-- query link: https://dune.com/queries/2836792


WITH lbp_info AS (
    SELECT *
    FROM query_2511450
    WHERE name = '{{LBP}}'
)

SELECT 
    CAST(day as timestamp) as day,
    COALESCE(b.token_symbol, SUBSTRING(CAST(token_address as VARCHAR), 0, 6)) AS symbol,
    pool_liquidity_usd as usd_amount
FROM balancer.liquidity b
INNER JOIN lbp_info l
ON l.pool_id = SUBSTRING(CAST(b.pool_id as VARCHAR), 1, 42) AND b.blockchain = l.blockchain
AND day <= end_time