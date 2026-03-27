-- part of a query repo
-- query name: Current vlAURA balances
-- query link: https://dune.com/queries/4004772


WITH user_locks AS (
    -- Fetch the locked amounts and their corresponding unlock times for each user
    SELECT 
        _user, 
        vlaura_round,
        _lockedAmount AS amount, 
        vlaura_round + 16 AS unlock_round
    FROM aura_finance_ethereum.AuraLocker_evt_Staked
    LEFT JOIN query_4001808 qa ON  --aura rounds
    (qa.start_date <= DATE_TRUNC('day', evt_block_time) AND qa.end_date > DATE_TRUNC('day', evt_block_time)) 

    UNION ALL

    -- Fetch the withdrawn amounts as negative values
    SELECT 
        _user, 
        vlaura_round,
        -(_amount) AS amount, 
        vlaura_round AS unlock_round
    FROM aura_finance_ethereum.AuraLocker_evt_Withdrawn
    LEFT JOIN query_4001808 qa ON  --aura rounds
    (qa.start_date <= DATE_TRUNC('day', evt_block_time) AND qa.end_date > DATE_TRUNC('day', evt_block_time)) 
),

aggregated_locks AS (
    -- Aggregate the locks to get the total locked amount and the cumulative sum of unlockable amounts
    SELECT 
        _user, 
        SUM(amount) AS total_vlaura,
        SUM(CASE WHEN unlock_round + 1 < (SELECT MAX(vlaura_round) FROM query_4001808) THEN amount ELSE 0 END) AS unlockable
    FROM user_locks
    GROUP BY _user
),


agg AS(
SELECT 
    qa.vlaura_round AS vlaura_round,
    qa.vebal_round,
    _user,
    COALESCE(qc.provider, 'Others') AS provider,
    total_vlaura / POWER(10,18) AS total_vlaura,
    unlockable / POWER(10,18) AS unlockable,
    (total_vlaura - unlockable) / POWER(10,18) AS total_locked,
    (SELECT SUM(total_vlaura - unlockable) FROM aggregated_locks) / POWER(10,18) AS agg_vlaura_locked,    qb.vebal_balance AS aura_vebal_balance
FROM aggregated_locks
LEFT JOIN query_4001808 qa ON  --aura rounds
(qa.start_date <= CURRENT_DATE AND qa.end_date > CURRENT_DATE) 
LEFT JOIN query_601405 qb ON qb.day = CURRENT_DATE --vebal balances
AND qb.provider = 'Aura' 
LEFT JOIN query_4007145 qc ON --aura providers
_user = qc.wallet_address

ORDER BY _user)

SELECT
    vlaura_round,
    vebal_round,
    provider,
    SUM(total_vlaura) AS total_vlaura,
    SUM(unlockable) AS unlockable,
    SUM(total_locked) AS total_locked,
    SUM(total_locked / agg_vlaura_locked) AS voting_power,
    SUM(aura_vebal_balance * (total_locked / agg_vlaura_locked)) AS vebal_balance
FROM agg
GROUP BY 1, 2, 3
ORDER BY 8 DESC