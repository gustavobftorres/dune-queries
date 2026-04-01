-- part of a query repo
-- query name: Total veBAL
-- query link: https://dune.com/queries/2638042


WITH deposits AS (
        SELECT
            provider,
            MIN(ts) AS locked_at,
            MAX(locktime) AS unlocked_at,
            SUM(CAST(value AS DOUBLE))/CAST(1e18 AS DOUBLE) AS bpt_locked
        FROM balancer_ethereum.veBAL_evt_Deposit
        GROUP BY 1
        ORDER BY 1, 2, 3
    ),
    
    withdrawals AS (
        SELECT 
            provider,
            SUM(CAST(value AS DOUBLE))/CAST(1e18 AS DOUBLE) AS bpt_unlocked
        FROM balancer_ethereum.veBAL_evt_Withdraw
        GROUP BY 1
        ORDER BY 1
    ),
    
    locks AS (
        SELECT
            d.provider,
            d.locked_at,
            d.unlocked_at,
            (d.unlocked_at - d.locked_at) AS lock_period,
            (CAST(d.bpt_locked AS DOUBLE) - COALESCE(CAST(w.bpt_unlocked AS DOUBLE), 0)) AS bpt_locked
        FROM deposits d
        LEFT JOIN withdrawals w
        ON w.provider = d.provider
        ORDER BY 1, 4
    ),
    
    vebal AS (
        SELECT
            provider,
            bpt_locked,
            FLOOR(to_unixtime(NOW())) AS ts_now,
            bpt_locked *
            (CAST(lock_period AS DOUBLE) / CAST(365*86400 AS DOUBLE)) *
            ((CAST(unlocked_at AS DOUBLE) - to_unixtime(NOW())) / CAST(lock_period AS DOUBLE)) AS vebal
        FROM locks
        ORDER BY 1
    )

SELECT
    ts_now,
    SUM(vebal) AS total_vebal
FROM vebal
GROUP BY 1
