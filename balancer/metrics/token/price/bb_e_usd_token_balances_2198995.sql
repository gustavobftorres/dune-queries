-- part of a query repo
-- query name: bb-e-USD token balances
-- query link: https://dune.com/queries/2198995


with swaps_changes AS (
    SELECT
        minute,
        pool_id,
        token,
        SUM(COALESCE(delta, 0)) AS delta
    FROM
        (
            SELECT
                date_trunc('minute', evt_block_time) AS minute,
                "poolId" AS pool_id,
                "tokenIn" AS token,
                "amountIn" AS delta
            FROM
                balancer_v2."Vault_evt_Swap"
            UNION
            ALL
            SELECT
                date_trunc('minute', evt_block_time) AS minute,
                "poolId" AS pool_id,
                "tokenOut" AS token,
                - "amountOut" AS delta
            FROM
                balancer_v2."Vault_evt_Swap"
        ) swaps
    GROUP BY 1,2,3
),
management_changes AS (
    SELECT
        minute,
        pool_id,
        token,
        SUM(COALESCE(delta, 0)) AS delta
    FROM
        (
            SELECT
                date_trunc('minute', evt_block_time) AS minute,
                "poolId" AS pool_id,
                token,
                "cashDelta" AS delta
            FROM
                balancer_v2."Vault_evt_PoolBalanceManaged"
            UNION
            ALL
            SELECT
                date_trunc('minute', evt_block_time) AS minute,
                "poolId" AS pool_id,
                token,
                "managedDelta" AS delta
            FROM
                balancer_v2."Vault_evt_PoolBalanceManaged"
        ) swaps
    GROUP BY 1,2,3
),
all_changes as (
    SELECT
        minute,
        pool_id,
        token,
        SUM(COALESCE(delta, 0)) AS delta
    FROM (
        SELECT * FROM swaps_changes
        UNION ALL
        SELECT * FROM management_changes
    ) changes
    GROUP BY 1,2,3
),
cumulative_balance_with_gaps AS (
        SELECT
            minute,
            pool_id,
            token,
            LEAD(minute, 1, NOW()) OVER (
                PARTITION BY token,
                pool_id
                ORDER BY
                    minute
            ) AS time_of_next_change,
            SUM(delta) OVER (
                PARTITION BY pool_id,
                token
                ORDER BY
                    minute ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) AS cumulative_amount
        FROM
            all_changes
    ),
    calendar AS (
        SELECT
            generate_series(
                (date_trunc('day', now()) - interval '5 days') :: timestamp,
                now(),
                '1 minute' :: INTERVAL
            ) AS minute
    ),
    cumulative_balance AS (
        SELECT
            c.minute,
            b.pool_id,
            b.token,
            cumulative_amount / 10 ^ coalesce(t.decimals,18) amount,
            t.symbol
        FROM
            calendar c
        LEFT JOIN cumulative_balance_with_gaps b ON b.minute <= c.minute
        AND c.minute < b.time_of_next_change
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        WHERE b.token != SUBSTRING(b.pool_id FOR 20)
    )
select a.minute, coalesce(b.symbol,a.token::text) as token, amount
-- , coalesce(volume, 0) as "1inch_volume" 
from cumulative_balance a
-- balancer_v2."view_liquidity"
left join erc20.tokens b
on a.token = b.contract_address
-- left join (
--     select time as minute, volume
--     from oneinchtrades
-- ) c
-- on a.minute = c.minute
where pool_id in (
-- where pool_id in (
    '\x3c640f0d3036ad85afa2d5a9e32be651657b874f00000000000000000000046b',
    '\xd4e7c1f3da1144c9e2cfd1b015eda7652b4a439900000000000000000000046a',
    '\xeb486af868aeb3b6e53066abc9623b1041b42bc000000000000000000000046c'
)
and token in (
'\xdac17f958d2ee523a2206206994597c13d831ec7',
'\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
'\x6b175474e89094c44da98b954eedeac495271d0f',
'\x4d19F33948b99800B6113Ff3e83beC9b537C85d2',
'\xEb91861f8A4e1C12333F42DCE8fB0Ecdc28dA716',
'\xe025E3ca2bE02316033184551D4d3Aa22024D9DC'
)