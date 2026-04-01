-- part of a query repo
-- query name: Balancer V2 LBP Balances
-- query link: https://dune.com/queries/151261


WITH lbp_info AS (
    SELECT *
    FROM dune_user_generated.balancer_v2_lbps
    WHERE name = '{{LBP}}'
)

SELECT 
    day,
    COALESCE(b.token_symbol, SUBSTRING(token_address::text, 0, 6)) AS symbol,
    usd_amount
FROM balancer_v2."view_liquidity" b
INNER JOIN lbp_info l
ON l.pool_id = b.pool_id
AND day <= end_time