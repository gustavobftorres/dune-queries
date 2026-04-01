-- part of a query repo
-- query name: vlAURA Delegations
-- query link: https://dune.com/queries/6675227


WITH rounds AS (
    SELECT
        vlaura_round,
        vebal_round,
        start_date,
        end_date
    FROM query_4001808
),
snapshot_overrides AS (
    SELECT from_address, to_address FROM (VALUES
        (0x2ad55394E12016c510D3C35d91Da7d90A758b7FD, 0x1f4F47f8b55CCd4e3b674Db90E4277b7073fdE27)
    ) AS t(from_address, to_address)
),
user_delegations AS (
    SELECT
        delegator,
        evt_block_time,
        delegate,
        ROW_NUMBER() OVER (
            PARTITION BY delegator 
            ORDER BY evt_block_number DESC, evt_index DESC
        ) as rn
    FROM (
        SELECT 
            delegator,
            delegate,
            evt_block_time,
            evt_block_number,
            evt_index
        FROM snapshot_ethereum.DelegateRegistry_evt_SetDelegate
        WHERE id IN (
            0x6175726166696e616e63652e6574680000000000000000000000000000000000,
            0x0000000000000000000000000000000000000000000000000000000000000000
        )
        
        UNION ALL
        
        SELECT 
            delegator,
            CAST(null AS VARBINARY) as delegate,
            evt_block_time,
            evt_block_number,
            evt_index
        FROM snapshot_ethereum.DelegateRegistry_evt_ClearDelegate
        WHERE id IN (
            0x6175726166696e616e63652e6574680000000000000000000000000000000000,
            0x0000000000000000000000000000000000000000000000000000000000000000
        )
    )
),
ethereum_locks AS (
    SELECT
        _user,
        CAST(_lockedAmount AS DOUBLE) AS amount,
        evt_block_time,
        FROM_UNIXTIME(
            (CAST(TO_UNIXTIME(evt_block_time) AS BIGINT) / 604800) * 604800 + (604800 * 17)
        ) as unlock_time
    FROM aura_finance_ethereum.AuraLocker_evt_Staked
    UNION ALL
    SELECT
        _user,
        -CAST(_amount AS DOUBLE) AS amount,
        evt_block_time,
        evt_block_time as unlock_time
    FROM aura_finance_ethereum.AuraLocker_evt_Withdrawn
),
base_locks AS (
    SELECT
        _user,
        CAST(_lockedAmount AS DOUBLE) AS amount,
        evt_block_time,
        FROM_UNIXTIME(
            (CAST(TO_UNIXTIME(evt_block_time) AS BIGINT) / 604800) * 604800 + (604800 * 17)
        ) as unlock_time
    FROM aura_finance_base.AuraLocker_evt_Staked
    UNION ALL
    SELECT
        _user,
        -CAST(_amount AS DOUBLE) AS amount,
        evt_block_time,
        evt_block_time as unlock_time
    FROM aura_finance_base.AuraLocker_evt_Withdrawn
),
ethereum_balances AS (
    SELECT 
        _user as address,
        SUM(
            CASE 
                WHEN unlock_time > CURRENT_TIMESTAMP THEN amount
                ELSE 0
            END
        ) / 1e18 as balance
    FROM ethereum_locks
    GROUP BY _user
),
base_balances AS (
    SELECT 
        _user as address,
        SUM(
            CASE 
                WHEN unlock_time > CURRENT_TIMESTAMP THEN amount
                ELSE 0
            END
        ) / 1e18 as balance
    FROM base_locks
    GROUP BY _user
),
current_balances AS (
    SELECT
        COALESCE(e.address, b.address) as delegator,
        COALESCE(e.balance, 0) as eth_locked,
        COALESCE(b.balance, 0) as base_locked
    FROM ethereum_balances e
    FULL OUTER JOIN base_balances b ON e.address = b.address
    WHERE COALESCE(e.balance, 0) > 0 OR COALESCE(b.balance, 0) > 0
)

SELECT
    cb.delegator,
    COALESCE(
        vebal.provider,
        vlaura.provider,
        sh.label,
        ens.name,
        CONCAT('0x', "LEFT"(TO_HEX(cb.delegator), 4), '...', "RIGHT"(TO_HEX(cb.delegator), 4))
    ) as delegator_label,
    cb.eth_locked,
    cb.base_locked,
    cb.eth_locked + cb.base_locked as total_locked,
    -- delegated_to only applies to Ethereum vlAURA
    COALESCE(ov.to_address, ud.delegate, cb.delegator) as delegated_to,
    COALESCE(
        vebal_d.provider,
        vlaura_d.provider,
        sh_d.label,
        ens_d.name,
        CONCAT('0x', "LEFT"(TO_HEX(COALESCE(ov.to_address, ud.delegate, cb.delegator)), 4), '...', "RIGHT"(TO_HEX(COALESCE(ov.to_address, ud.delegate, cb.delegator)), 4))
    ) as delegated_to_label,
    CASE 
        WHEN ud.delegate IS NOT NULL AND ud.delegate != cb.delegator THEN true 
        ELSE false 
    END as has_delegated
FROM current_balances cb
LEFT JOIN user_delegations ud ON cb.delegator = ud.delegator AND ud.rn = 1
LEFT JOIN snapshot_overrides ov ON ov.from_address = ud.delegate
LEFT JOIN query_3032958 vebal ON vebal.wallet_address = cb.delegator
LEFT JOIN query_3032958 vebal_d ON vebal_d.wallet_address = COALESCE(ov.to_address, ud.delegate, cb.delegator)
LEFT JOIN query_4007145 vlaura ON vlaura.wallet_address = cb.delegator
LEFT JOIN query_4007145 vlaura_d ON vlaura_d.wallet_address = COALESCE(ov.to_address, ud.delegate, cb.delegator)
LEFT JOIN query_6675327 sh ON sh.address = cb.delegator
LEFT JOIN query_6675327 sh_d ON sh_d.address = COALESCE(ov.to_address, ud.delegate, cb.delegator)
LEFT JOIN labels.ens ens ON ens.address = cb.delegator AND ens.blockchain = 'ethereum'
LEFT JOIN labels.ens ens_d ON ens_d.address = COALESCE(ov.to_address, ud.delegate, cb.delegator) AND ens_d.blockchain = 'ethereum'
ORDER BY total_locked DESC
