-- part of a query repo
-- query name: vlAURA_balances_round
-- query link: https://dune.com/queries/4023187


WITH rounds AS (
    SELECT
        vlaura_round,
        vebal_round,
        start_date,
        end_date
    FROM
        query_4001808
),

user_delegations AS (
    SELECT
        delegator AS _user,
        evt_block_time,
        qa.vlaura_round + 1 AS vlaura_round, --as delegations must be done for next epoch
        toDelegate AS delegate,
        ROW_NUMBER() OVER (PARTITION BY qa.vlaura_round, delegator ORDER BY evt_block_time DESC) AS rn,
                LEAD(vlaura_round + 1, 1, (
            SELECT
                MAX(vlaura_round) + 1
            FROM
                rounds
        )) OVER (
            PARTITION BY delegator
            ORDER BY vlaura_round
        ) AS round_of_next_change
    FROM
        aura_finance_ethereum.AuraLocker_evt_DelegateChanged AS st
    LEFT JOIN
        rounds AS qa
        ON qa.start_date <= DATE_TRUNC('day', st.evt_block_time)
        AND qa.end_date > DATE_TRUNC('day', st.evt_block_time)
),

user_locks AS (
    SELECT
        _user,
        COALESCE(qa.vlaura_round + 1, (SELECT
                MAX(vlaura_round)
            FROM
                rounds)) AS vlaura_round,
        _lockedAmount AS amount,
        qa.vlaura_round + 17 AS unlock_round
    FROM
        aura_finance_ethereum.AuraLocker_evt_Staked AS st
    LEFT JOIN
        rounds AS qa
        ON qa.start_date <= DATE_TRUNC('day', st.evt_block_time)
        AND qa.end_date > DATE_TRUNC('day', st.evt_block_time)

    UNION ALL

    SELECT
        _user,
        qa.vlaura_round,
        -(_amount) AS amount,
        qa.vlaura_round AS unlock_round
    FROM
        aura_finance_ethereum.AuraLocker_evt_Withdrawn AS wt
    LEFT JOIN
        rounds AS qa
        ON qa.start_date <= DATE_TRUNC('day', wt.evt_block_time)
        AND qa.end_date > DATE_TRUNC('day', wt.evt_block_time)
),

aggregated_locks AS (
    SELECT DISTINCT
        _user,
        vlaura_round,
        LEAD(vlaura_round, 1, (
            SELECT
                MAX(vlaura_round) + 1
            FROM
                rounds
        )) OVER (
            PARTITION BY _user
            ORDER BY vlaura_round
        ) AS round_of_next_change,
        SUM(amount) OVER (
            PARTITION BY _user
            ORDER BY vlaura_round
        ) AS total_vlaura,
        SUM(
            CASE
                WHEN unlock_round < vlaura_round THEN amount
                ELSE 0
            END
        ) OVER (
            PARTITION BY _user
            ORDER BY vlaura_round
        ) AS unlockable
    FROM
        user_locks
),

full_round_user AS (
    SELECT
        rs.vlaura_round,
        rs.vebal_round,
        u._user
    FROM (
        SELECT DISTINCT
            vlaura_round,
            vebal_round
        FROM
            rounds
    ) AS rs
    CROSS JOIN (
        SELECT DISTINCT
            _user
        FROM
            aggregated_locks
    ) AS u
),

agg AS (
    SELECT
        fru.vlaura_round,
        fru.vebal_round,
        fru._user,
        COALESCE(qc.provider, TRY_CAST(BYTEARRAY_SUBSTRING(fru._user, 1, 3) AS VARCHAR)) AS provider,
        COALESCE(ud.delegate, fru._user) AS delegate,
        COALESCE(al.total_vlaura / POWER(10, 18), 0) AS total_vlaura,
        GREATEST(al.unlockable / POWER(10, 18), 0) AS unlockable,
        COALESCE(
            (al.total_vlaura - GREATEST(al.unlockable, 0)) / POWER(10, 18),
            0
        ) AS total_locked,
        COALESCE(
            SUM(al.total_vlaura - GREATEST(al.unlockable, 0)) OVER (
                PARTITION BY fru.vlaura_round
            ) / POWER(10, 18),
            0
        ) AS agg_vlaura_locked,
        qb.vebal_balance AS aura_vebal_balance
    FROM
        full_round_user AS fru
    LEFT JOIN
        aggregated_locks AS al
        ON al.vlaura_round <= fru.vlaura_round
        AND fru.vlaura_round < al.round_of_next_change
        AND fru._user = al._user
    LEFT JOIN
        rounds AS qa
        ON qa.vlaura_round = fru.vlaura_round
    LEFT JOIN
        query_601405 AS qb
        ON qb.day = qa.start_date + INTERVAL '1' DAY
        AND qb.provider = 'Aura'
    LEFT JOIN
        query_4007145 AS qc
        ON fru._user = qc.wallet_address
    LEFT JOIN 
        user_delegations AS ud
        ON fru._user = ud._user
        AND (fru.vlaura_round >= ud.vlaura_round)
        AND ud.rn = 1
        AND ud.round_of_next_change > fru.vlaura_round
)

SELECT
    vlaura_round,
    vebal_round,
    _user,
    provider,
    delegate,
    SUM(total_vlaura) AS total_vlaura,
    SUM(unlockable) AS unlockable,
    SUM(total_locked) AS total_locked,
    SUM(total_locked / agg_vlaura_locked) AS voting_power,
    SUM(aura_vebal_balance * (total_locked / agg_vlaura_locked)) AS vebal_balance
FROM
    agg
GROUP BY
    1, 2, 3, 4, 5
ORDER BY
    1 DESC,  8 DESC