-- part of a query repo
-- query name: vlAura locked over time by wallet
-- query link: https://dune.com/queries/3999509


WITH getAura AS (
    SELECT 
        date, 
        _user, 
        SUM(amount) AS daily_changes,
        SUM(SUM(amount)) OVER (PARTITION BY _user ORDER BY date) AS cumulative_total 
    FROM (
        SELECT 
            date_trunc('week', evt_block_time) - interval '4' day AS date, 
            _user, 
            _lockedAmount / 1e18 AS amount 
        FROM aura_finance_ethereum.AuraLocker_evt_Staked 
        UNION ALL
        SELECT 
            date_trunc('week', evt_block_time) - interval '4' day AS date, 
            _user, 
            -(_amount / 1e18) AS amount 
        FROM aura_finance_ethereum.AuraLocker_evt_Withdrawn 
    ) a
    GROUP BY 1, 2
    ORDER BY 1 DESC
),

user_locks AS (
    SELECT 
        _user, 
        _lockedAmount / 1e18 AS amount, 
        evt_block_time,
        evt_block_time + interval '112' day AS unlock_time
    FROM aura_finance_ethereum.AuraLocker_evt_Staked

    UNION ALL

    SELECT 
        _user, 
        -(_amount / 1e18) AS amount, 
        evt_block_time,
        evt_block_time AS unlock_time
    FROM aura_finance_ethereum.AuraLocker_evt_Withdrawn
),

user_locks_2 AS (
    SELECT 
        *,
        COALESCE(LEAD(evt_block_time, 1, NOW()) OVER (PARTITION BY _user ORDER BY evt_block_time), CURRENT_TIMESTAMP) AS next_change
    FROM user_locks
),

aggregated_locks AS (
    SELECT 
        date_trunc('week', evt_block_time) - interval '4' day AS date,
        _user, 
        SUM(amount) AS total_locked,
        SUM(CASE WHEN unlock_time <= CURRENT_TIMESTAMP THEN amount ELSE 0 END) AS unlockable
    FROM user_locks
    GROUP BY 1, 2
),

getAuraPrice AS (
    SELECT 
        date_trunc('week', minute) - interval '4' day AS date,
        AVG(price) AS avg_price 
    FROM prices.usd
    WHERE blockchain = 'ethereum'
    AND symbol = 'AURA'
    GROUP BY 1
)

SELECT 
    a.date,
    a._user,
    a.cumulative_total,
    a.daily_changes,
    agg_locks.total_locked,
    agg_locks.unlockable,
    agg_locks.total_locked - agg_locks.unlockable AS active_vlaura,
    b.avg_price,
    q.vlaura_round, 
    q.vebal_round, 
    b.avg_price * a.cumulative_total AS total_usd,
    b.avg_price * a.daily_changes AS daily_changes_usd 
FROM getAura a 
LEFT JOIN getAuraPrice b ON a.date = b.date
LEFT JOIN aggregated_locks agg_locks ON a._user = agg_locks._user
AND a.date = agg_locks.date
LEFT JOIN query_4001808 q ON a.date = q.start_date
WHERE b.avg_price IS NOT NULL
ORDER BY a.date DESC;
