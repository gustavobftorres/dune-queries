-- part of a query repo
-- query name: LST / LRT DEX by volume
-- query link: https://dune.com/queries/3949964


WITH
  thirty_day_volume AS (
    SELECT
      project AS "Project",
      SUM(CAST(amount_usd AS DOUBLE)) AS usd_volume
    FROM
      dex."trades" AS t
    INNER JOIN dune.balancer.result_lst_tokens l ON t.blockchain = l.blockchain 
    AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
    WHERE block_time > NOW() - INTERVAL '30' day
    AND t.blockchain IN (SELECT DISTINCT blockchain FROM balancer.liquidity)
    AND ('{{Blockchain}}' = 'All' OR t.blockchain = '{{Blockchain}}')
    AND ('{{Token Pair}}' = 'All' OR token_pair = '{{Token Pair}}')
    GROUP BY
      1
  ),

  seven_day_volume AS (
    SELECT
      project AS "Project",
      SUM(CAST(amount_usd AS DOUBLE)) AS usd_volume
    FROM
      dex."trades" AS t
    INNER JOIN dune.balancer.result_lst_tokens l ON t.blockchain = l.blockchain 
    AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
    WHERE block_time > NOW() - INTERVAL '7' day
    AND t.blockchain IN (SELECT DISTINCT blockchain FROM balancer.liquidity)
    AND ('{{Blockchain}}' = 'All' OR t.blockchain = '{{Blockchain}}')
    AND ('{{Token Pair}}' = 'All' OR token_pair = '{{Token Pair}}')
    GROUP BY
      1
  ),
  one_day_volume AS (
    SELECT
      project AS "Project",
      SUM(CAST(amount_usd AS DOUBLE)) AS usd_volume
    FROM
      dex."trades" AS t
    INNER JOIN dune.balancer.result_lst_tokens l ON t.blockchain = l.blockchain 
    AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
    WHERE
      block_time > NOW() - INTERVAL '1' day
    AND t.blockchain IN (SELECT DISTINCT blockchain FROM balancer.liquidity)
    AND ('{{Blockchain}}' = 'All' OR t.blockchain = '{{Blockchain}}')  
    AND ('{{Token Pair}}' = 'All' OR token_pair = '{{Token Pair}}')
    GROUP BY
      1
  )
SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      SUM(thirty.usd_volume) DESC NULLS FIRST
  ) AS "Rank",
  thirty."Project",
  SUM(thirty.usd_volume) AS "30 Days Volume",
  SUM(seven.usd_volume) AS "7 Days Volume",
  SUM(one.usd_volume) AS "24 Hours Volume"
FROM thirty_day_volume AS thirty
  LEFT JOIN seven_day_volume AS seven ON thirty."Project" = seven."Project"
  LEFT JOIN one_day_volume AS one ON thirty."Project" = one."Project"
WHERE
  NOT thirty.usd_volume IS NULL
GROUP BY
  2
ORDER BY
  3 DESC NULLS FIRST