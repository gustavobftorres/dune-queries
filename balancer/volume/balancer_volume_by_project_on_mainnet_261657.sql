-- part of a query repo
-- query name: Balancer Volume by Project on Mainnet
-- query link: https://dune.com/queries/261657


WITH projects AS (
        SELECT 
            name,
            address
        FROM labels.labels
        WHERE "type" = 'balancer_project'
        AND author IN ('balancerlabs', 'metacrypto', 'markusbkoch', 'mangool', 'astivelman')
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