-- part of a query repo
-- query name: Balancer V2 LBP Stats
-- query link: https://dune.com/queries/151187


WITH lbp_info AS (
        SELECT *
        FROM  dune_user_generated.balancer_v2_lbps
        WHERE name = '{{LBP}}'
    )

SELECT 
    end_time - start_time AS duration,
    COUNT(DISTINCT trader_a) AS participants, 
    (SELECT COUNT(*) FROM dex."trades" t
    INNER JOIN lbp_info l ON l.pool_id = t.exchange_contract_address
    WHERE project = 'Balancer' ) AS txns,
    (SELECT SUM(usd_amount) FROM dex."trades" t
    INNER JOIN lbp_info l ON l.pool_id = t.exchange_contract_address
    WHERE project = 'Balancer' 
    AND block_time <= l.end_time) AS volume
FROM dex."trades" t
INNER JOIN lbp_info l ON l.pool_id = t.exchange_contract_address
AND l.token_sold = t.token_a_address
WHERE project = 'Balancer'
AND block_time <= l.end_time
AND block_time >= l.start_time
GROUP BY 1