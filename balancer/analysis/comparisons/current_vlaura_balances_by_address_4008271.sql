-- part of a query repo
-- query name: Current vlAURA balances by address
-- query link: https://dune.com/queries/4008271


WITH user_lock_events AS (
    -- Fetch locked amounts and their corresponding unlock rounds for each user
    SELECT 
        _user, 
        vlaura_round,
        _lockedAmount AS locked_amount, 
        vlaura_round + 17 AS unlock_round
    FROM aura_finance_ethereum.AuraLocker_evt_Staked
    LEFT JOIN query_4001808 aura_rounds -- aura rounds
    ON aura_rounds.start_date <= DATE_TRUNC('day', evt_block_time) 
       AND aura_rounds.end_date > DATE_TRUNC('day', evt_block_time)

    UNION ALL

    -- Fetch withdrawn amounts as negative values
    SELECT 
        _user, 
        vlaura_round,
        -(_amount) AS locked_amount, 
        vlaura_round AS unlock_round
    FROM aura_finance_ethereum.AuraLocker_evt_Withdrawn
    LEFT JOIN query_4001808 aura_rounds -- aura rounds
    ON aura_rounds.start_date <= DATE_TRUNC('day', evt_block_time) 
       AND aura_rounds.end_date > DATE_TRUNC('day', evt_block_time)
),

aggregated_locks AS (
    -- Aggregate locks to get total locked amount and cumulative unlockable amounts
    SELECT 
        _user, 
        SUM(locked_amount) AS total_locked_amount,
        SUM(
            CASE 
                WHEN unlock_round <= (SELECT MAX(vlaura_round) FROM query_4001808) 
                THEN locked_amount 
                ELSE 0 
            END
        ) AS total_unlockable_amount
    FROM user_lock_events
    GROUP BY _user
),

final_data AS (
    SELECT 
        aura_rounds.vlaura_round AS vlaura_round,
        aura_rounds.vebal_round,
        _user,
        COALESCE(aura_providers.provider, 'others') AS provider,
        total_locked_amount / POWER(10, 18) AS total_locked_amount,
        CASE WHEN total_unlockable_amount > 0
        THEN total_unlockable_amount / POWER(10, 18) 
        ELSE 0
        END AS total_unlockable_amount,
        CASE WHEN total_unlockable_amount > 0
        THEN (total_locked_amount - total_unlockable_amount) / POWER(10, 18) 
        ELSE total_locked_amount / POWER(10, 18)
        END AS total_locked_balance,
        vebal_balances.vebal_balance AS aura_vebal_balance
    FROM aggregated_locks
    LEFT JOIN query_4001808 aura_rounds -- aura rounds
        ON aura_rounds.start_date <= CURRENT_DATE 
        AND aura_rounds.end_date > CURRENT_DATE 
    LEFT JOIN query_601405 vebal_balances -- vebal balances
        ON vebal_balances.day = CURRENT_DATE 
        AND vebal_balances.provider = 'Aura' 
    LEFT JOIN query_4007145 aura_providers -- aura providers
        ON _user = aura_providers.wallet_address
    ORDER BY _user
)

SELECT
    vlaura_round,
    vebal_round,
    _user,
    provider,
    SUM(total_locked_amount) AS total_vlaura,
    SUM(total_unlockable_amount) AS unlockable,
    SUM(total_locked_balance) AS total_locked,
    SUM(total_locked_balance / (SELECT SUM(total_locked_balance) FROM final_data)) AS voting_power,
    SUM(aura_vebal_balance * (total_locked_balance / (SELECT SUM(total_locked_balance) FROM final_data))) AS vebal_balance
FROM final_data
GROUP BY 1, 2, 3, 4
ORDER BY 8 DESC;
