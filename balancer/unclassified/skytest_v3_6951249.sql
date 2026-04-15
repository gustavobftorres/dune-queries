-- part of a query repo
-- query name: skytest_v3
-- query link: https://dune.com/queries/6951249



SELECT 
  CONCAT('0x', LOWER(TO_HEX(c.address))) as address,
  LENGTH(c.code)/2 as code_size
FROM bnb.contracts c
WHERE LENGTH(c.code)/2 BETWEEN 15000 AND 30000
  AND STRPOS(CAST(c.code AS VARCHAR), '4b8a3529') > 0
  AND STRPOS(CAST(c.code AS VARCHAR), '338b5dea') > 0
  AND STRPOS(CAST(c.code AS VARCHAR), '27a741ec') > 0
LIMIT 200
