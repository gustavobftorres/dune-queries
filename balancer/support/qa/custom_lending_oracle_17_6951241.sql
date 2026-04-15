-- part of a query repo
-- query name: custom_lending_oracle_17
-- query link: https://dune.com/queries/6951241



SELECT 
  blockchain, address, 
  length(code)/2 as code_size
FROM evms.creation_traces t
JOIN evms.contracts c ON t.address = c.address AND t.blockchain = c.blockchain
WHERE t.blockchain IN ('bnb', 'ethereum', 'polygon', 'arbitrum', 'fantom', 'avalanche_c')
  AND t.success = true
  AND length(c.code)/2 BETWEEN 15000 AND 30000
  AND position(x'4b8a3529' in c.code) > 0
  AND (
    position(x'338b5dea' in c.code) > 0
    OR position(x'47e7ef24' in c.code) > 0
  )
  AND (
    position(x'27a741ec' in c.code) > 0
    OR position(x'f11993df' in c.code) > 0
    OR position(x'98d5fdca' in c.code) > 0
  )
LIMIT 500
