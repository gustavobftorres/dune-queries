-- part of a query repo
-- query name: Balancer V3 LBP Balances
-- query link: https://dune.com/queries/4838042


WITH lbp_info AS (
        SELECT *
        FROM  query_4837895
        WHERE pool_symbol = '{{LBP}}'
        AND blockchain = '{{blockchain}}'
)

SELECT 
    CAST(day as timestamp) as day,
    COALESCE(b.token_symbol, SUBSTRING(CAST(token_address as VARCHAR), 0, 6)) AS symbol,
    pool_liquidity_usd as usd_amount
FROM balancer.liquidity b
INNER JOIN lbp_info l
ON l.pool_address = b.pool_address AND b.blockchain = l.blockchain
AND day <= end_time