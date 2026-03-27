-- part of a query repo
-- query name: vlAURA Breakdown
-- query link: https://dune.com/queries/6672391


SELECT
    provider,
    CONCAT('0x', to_hex(_user)) AS wallet,
    SUM(total_locked) AS locked_vlaura,
    SUM(unlockable) AS unlockable_vlaura,
    SUM(voting_power) AS voting_power_pct,
    SUM(vebal_balance) AS vebal_equivalent
FROM query_4023187
WHERE vebal_round = (SELECT MAX(vebal_round) - 1 FROM query_4023187)
  AND total_locked > 0
GROUP BY 1, 2
ORDER BY 6 DESC