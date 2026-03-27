-- part of a query repo
-- query name: veBAL Metrics (Dune SQL)
-- query link: https://dune.com/queries/2150682


WITH total_balances AS (
  SELECT
    day,
    sum(total) OVER (ORDER BY day) AS total
  FROM (
    SELECT
      date_trunc('day', evt_block_time) AS day,
      SUM(value / 1e18) AS total
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      CAST(contract_address AS VARCHAR(42)) = '0x5c6ee304399dbdb9c8ef030ab642b10820db8f56'
    AND
        CAST("from" AS VARCHAR(42)) = '0x0000000000000000000000000000000000000000'
    GROUP BY 1
    UNION ALL
    SELECT
      date_trunc('day', evt_block_time) as day,
      -sum(value / 1e18) AS total
    FROM erc20_ethereum.evt_Transfer
    WHERE CAST(contract_address AS VARCHAR(42)) = '0x5c6ee304399dbdb9c8ef030ab642b10820db8f56'
    AND CAST("to" AS VARCHAR(42)) = '0x0000000000000000000000000000000000000000'
    GROUP BY 1
    ) foo
),
locked_balances AS (
    SELECT
        day,
        SUM(bpt_balance) AS locked,
        0 AS total
    FROM
      --vebal_balances_day
      query_2276840
    GROUP BY 1
)
    
SELECT
    t.day,
    l.locked,
    t.total,
    l.locked / t.total AS locked_pct,
    l.locked / t.total * 100 AS locked_pct_2
FROM total_balances t
JOIN locked_balances l
ON t.day = l.day
ORDER BY 1 DESC