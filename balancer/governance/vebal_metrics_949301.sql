-- part of a query repo
-- query name: veBAL metrics
-- query link: https://dune.com/queries/949301


with swaps_changes AS (
        SELECT
            DAY,
            pool_id,
            token,
            SUM(COALESCE(delta, 0)) AS delta
        FROM
            (
                SELECT
                    date_trunc('day', evt_block_time) AS DAY,
                    "poolId" AS pool_id,
                    "tokenIn" AS token,
                    "amountIn" AS delta
                FROM
                    balancer_v2."Vault_evt_Swap"
                UNION
                ALL
                SELECT
                    date_trunc('day', evt_block_time) AS DAY,
                    "poolId" AS pool_id,
                    "tokenOut" AS token,
                    - "amountOut" AS delta
                FROM
                    balancer_v2."Vault_evt_Swap"
            ) swaps
        GROUP BY
            1,
            2,
            3
    ),
    balances_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS DAY,
            "poolId" AS pool_id,
            UNNEST(tokens) AS token,
            UNNEST(deltas) - UNNEST("protocolFeeAmounts") AS delta
        FROM
            balancer_v2."Vault_evt_PoolBalanceChanged"
    ),
    managed_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS DAY,
            "poolId" AS pool_id,
            token,
            "cashDelta" + "managedDelta" AS delta
        FROM
            balancer_v2."Vault_evt_PoolBalanceManaged"
    ),
    daily_delta_balance AS (
        SELECT
            DAY,
            pool_id,
            token,
            SUM(COALESCE(amount, 0)) AS amount
        FROM
            (
                SELECT
                    DAY,
                    pool_id,
                    token,
                    SUM(COALESCE(delta, 0)) AS amount
                FROM
                    balances_changes
                GROUP BY
                    1,
                    2,
                    3
                UNION
                ALL
                SELECT
                    DAY,
                    pool_id,
                    token,
                    delta AS amount
                FROM
                    swaps_changes
                UNION
                ALL
                SELECT
                    DAY,
                    pool_id,
                    token,
                    delta AS amount
                FROM
                    managed_changes
            ) balance
        GROUP BY
            1,
            2,
            3
    ),
    cumulative_balance AS (
        SELECT
            pool_id,
            token,
            SUM(amount) as balance
        FROM
            daily_delta_balance
        GROUP BY 1,2
    )
    select balance/1e18 as balance from cumulative_balance
    where pool_id = '\x5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014'
    and token = '\xba100000625a3754423978a60c9317c58a424e3d'