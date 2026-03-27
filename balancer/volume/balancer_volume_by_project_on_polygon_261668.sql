-- part of a query repo
-- query name: Balancer Volume by Project on Polygon
-- query link: https://dune.com/queries/261668


WITH projects AS (
        SELECT 'trueusd' AS name, '\x0d34e5dd4d8f043557145598e4e2dc286b35fd4f'::bytea AS address
        UNION ALL
        SELECT 'dhedge' AS name, '\x5028497af0c9a54ea8c6d42a054c0341b9fc6168'::bytea AS address
        -- FROM labels.labels
        -- WHERE "type" = 'balancer_project'
        -- AND author IN ('balancerlabs', 'metacrypto', 'markusbkoch', 'mangool', 'astivelman')
    )

SELECT 
    p.name,
    date_trunc('week', block_time) AS week,
    SUM(usd_amount) AS volume
FROM dex.trades d
JOIN projects p 
ON p.address = SUBSTRING(exchange_contract_address, 0, 21)
AND d.project = 'Balancer'
AND block_time >= '{{1. Start date}}'
AND block_time <= '{{2. End date}}'
GROUP BY 1, 2