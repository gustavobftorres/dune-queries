-- part of a query repo
-- query name: Daily MEV Captured by pool
-- query link: https://dune.com/queries/4734863


SELECT
block_date,
      CASE 
        WHEN t.blockchain = 'arbitrum' THEN ' 🟦'
        WHEN t.blockchain = 'ethereum' THEN ' Ξ'
        WHEN t.blockchain = 'gnosis' THEN ' 🟩'
        WHEN t.blockchain = 'base' THEN ' 🟨'
      END
      || pool_symbol
        AS pool,
  SUM(mev_captured) AS mev_captured
FROM query_4734806 t
WHERE mev_captured > 0
GROUP BY 1, 2