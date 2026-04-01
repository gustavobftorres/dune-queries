-- part of a query repo
-- query name: Boosted Pool Mapping
-- query link: https://dune.com/queries/4419172


SELECT  name, 
        address, 
        blockchain,
        CASE WHEN (name LIKE '%aave%' OR name LIKE '%wa%' OR NAME LIKE '%eure%')
        THEN 'aave'
        WHEN (NAME LIKE '%cs%' OR name LIKE '%pxeth%' OR name LIKE '%pfeth%' OR name LIKE '%lpeth%') 
        THEN 'morpho'
        ELSE 'others'
        END AS lending_market
FROM labels.balancer_v3_pools
WHERE NAME LIKE '%wa%'
OR NAME LIKE '%aave%'
OR NAME LIKE '%eure%'
OR NAME LIKE '%cs%'
OR NAME LIKE '%pxeth%'
--OR NAME LIKE '%sdai%'