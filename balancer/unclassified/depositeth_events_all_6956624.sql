-- part of a query repo
-- query name: depositeth_events_all
-- query link: https://dune.com/queries/6956624



SELECT blockchain, 
       CAST(contract_address AS VARCHAR) as address,
       COUNT(*) as deposit_count
FROM evms.logs
WHERE topic0 = 0x294738b98bcebacf616fd72532d3d8d8d229807bf03b68b25681bfbbdb3d3fe5
  AND blockchain NOT IN ('ethereum', 'bnb')
GROUP BY 1, 2
ORDER BY deposit_count DESC
LIMIT 200
