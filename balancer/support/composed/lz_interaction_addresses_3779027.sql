-- part of a query repo
-- query name: LZ interaction addresses
-- query link: https://dune.com/queries/3779027


WITH addresses AS (
  SELECT DISTINCT
    a.user_address,
    a.blockchain,
    CASE
      WHEN a.blockchain = 'ethereum' THEN 100
      WHEN a.blockchain = 'avalanche_c' THEN 106
    END AS lz_chain_id
  FROM (
    SELECT
      user_address,
      blockchain
    FROM query_3779014 /* vebal boosters */
    
    UNION ALL
    
    SELECT
      user_address,
      blockchain
    FROM query_3778080 /* BAL bridgers */
  ) AS a
)

SELECT
    a.user_address,
    a.blockchain,
    a.lz_chain_id,
    ROW_NUMBER() OVER () AS rn
FROM addresses a