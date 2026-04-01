-- part of a query repo
-- query name: BAL Holders and LPs (Dune SQL)
-- query link: https://dune.com/queries/2828007


WITH transfers AS (
    SELECT
    evt_tx_hash AS tx_hash,
    tr."from" AS address,
    - 1 * CAST(tr.value as double) AS amount,
    contract_address
    FROM erc20_ethereum.evt_Transfer tr
    WHERE contract_address =  0xba100000625a3754423978a60c9317c58a424e3d
UNION ALL
    SELECT
    evt_tx_hash AS tx_hash,
    tr."to" AS address,
    CAST(tr.value as double) AS amount,
     contract_address
     FROM erc20_ethereum.evt_Transfer tr 
     where contract_address = 0xba100000625a3754423978a60c9317c58a424e3d
),
transferAmounts AS (
    SELECT address,
    
    sum(amount)/1e18 as poolholdings FROM transfers 
    
    GROUP BY 1
    ORDER BY 2 DESC
)

SELECT COUNT(DISTINCT uniques) uniques FROM (
SELECT 
DISTINCT address as uniques
FROM transferAmounts
WHERE poolholdings > 0
INTERSECT
SELECT DISTINCT caller as uniques
FROM
balancer_v1_ethereum.BPool_evt_LOG_JOIN) a