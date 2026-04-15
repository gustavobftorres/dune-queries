-- part of a query repo
-- query name: depositeth_all_v2
-- query link: https://dune.com/queries/6957432



SELECT blockchain,
       CAST(contract_address AS VARCHAR) as address,
       COUNT(*) as cnt
FROM evms.logs
WHERE topic0 = 0x294738b98bcebacf616fd72532d3d8d8d229807bf03b68b25681bfbbdb3d3fe5
GROUP BY 1, 2
ORDER BY cnt DESC
LIMIT 500
