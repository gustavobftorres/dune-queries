-- part of a query repo
-- query name: COW/WETH Volume on Base
-- query link: https://dune.com/queries/5616702


SELECT 
    block_date,
    CASE 
        WHEN project = 'balancer' THEN 'Balancer'
        ELSE 'Others'
    END AS project,
    SUM(amount_usd) AS volume_usd
FROM dex.trades
WHERE blockchain = 'base'
AND token_pair IN ('COW-WETH', 'WETH-COW')
AND block_date >= timestamp '2025-07-16'
AND project_contract_address != 0x3a2E1aa0e67a1d32BEe33F8322F010FEdE37A3d1 -- COW isn't CoW Swap token
GROUP BY 1, 2
ORDER BY 1, 2
