-- part of a query repo
-- query name: skytest_clones_bsc_17
-- query link: https://dune.com/queries/6951244



SELECT 
  'bnb' as blockchain,
  CONCAT('0x', ENCODE(c.address, 'hex')) as address,
  LENGTH(c.code)/2 as code_size
FROM bnb.contracts c
WHERE LENGTH(c.code)/2 BETWEEN 15000 AND 30000
  AND POSITION(x'4b8a3529' IN c.code) > 0
  AND POSITION(x'338b5dea' IN c.code) > 0
  AND POSITION(x'27a741ec' IN c.code) > 0
LIMIT 200
