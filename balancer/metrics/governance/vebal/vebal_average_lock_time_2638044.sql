-- part of a query repo
-- query name: veBAL Average Lock Time
-- query link: https://dune.com/queries/2638044


WITH
    last_locked AS (
        SELECT
            provider,
            MAX(locktime) - MAX(ts) AS lock_time
        FROM
            balancer_ethereum.veBAL_evt_Deposit
        GROUP BY
        1
    ),
    positive_balances AS (
        SELECT
            wallet_address AS provider,
            vebal_balance,
            bpt_balance
        FROM
            --vebal_balances_day
            query_2276840
        WHERE day = DATE_TRUNC('day', NOW())
        AND vebal_balance > 0
    )

SELECT AVG(lock_time) / (365 * 86400 / 12) AS avg_lock_time
FROM last_locked AS l
INNER JOIN positive_balances AS p ON l.provider = p.provider