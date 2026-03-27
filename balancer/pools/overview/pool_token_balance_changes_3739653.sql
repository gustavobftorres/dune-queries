-- part of a query repo
-- query name: Pool token balance changes
-- query link: https://dune.com/queries/3739653


WITH pool_labels AS (
        SELECT
            address AS pool_id,
            name AS pool_symbol,
            pool_type
        FROM labels.balancer_v2_pools
        WHERE blockchain = 'arbitrum'
    ),

    swaps_changes AS (
        SELECT
            evt_block_time,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            pool_id,
            token,
            SUM(COALESCE(delta, INT256 '0')) AS delta
        FROM
            (
                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    poolId AS pool_id,
                    tokenIn AS token,
                    CAST(amountIn as int256) AS delta
                FROM balancer_v2_arbitrum.Vault_evt_Swap

                UNION ALL

                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    poolId AS pool_id,
                    tokenOut AS token,
                    -CAST(amountOut AS int256) AS delta
                FROM balancer_v2_arbitrum.Vault_evt_Swap
            ) swaps
        GROUP BY 1, 2, 3, 4, 5, 6
    ),

    zipped_balance_changes AS (
        SELECT
            evt_block_time,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            poolId AS pool_id,
            t.tokens,
            d.deltas,
            p.protocolFeeAmounts
        FROM balancer_v2_arbitrum.Vault_evt_PoolBalanceChanged
        CROSS JOIN UNNEST (tokens) WITH ORDINALITY as t(tokens,i)
        CROSS JOIN UNNEST (deltas) WITH ORDINALITY as d(deltas,i)
        CROSS JOIN UNNEST (protocolFeeAmounts) WITH ORDINALITY as p(protocolFeeAmounts,i)
        WHERE t.i = d.i 
        AND d.i = p.i
    ),

    balances_changes AS (
        SELECT
            evt_block_time,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            pool_id,
            tokens AS token,
            deltas - CAST(protocolFeeAmounts as int256) AS delta
        FROM zipped_balance_changes
    ),

    managed_changes AS (
        SELECT
            evt_block_time,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            poolId AS pool_id,
            token,
            cashDelta + managedDelta AS delta
        FROM balancer_v2_arbitrum.Vault_evt_PoolBalanceManaged
    )


        SELECT
            date_trunc('day', b.evt_block_time) AS block_date,
            b.evt_block_time,
            b.evt_block_number,
            'arbitrum' AS blockchain,
            b.evt_tx_hash,
            b.evt_index,
            b.pool_id,
            BYTEARRAY_SUBSTRING(b.pool_id, 1, 20) AS pool_address,
            p.pool_symbol,
            p.pool_type,
            '{{version}}' AS version, 
            b.token AS token_address,
            t.symbol AS token_symbol,
            b.amount AS delta_amount_raw,
            CASE WHEN BYTEARRAY_SUBSTRING(b.pool_id, 1, 20) = b.token
            THEN amount / POWER (10, 18) --for Balancer Pool Tokens
            ELSE amount / POWER (10, COALESCE(t.decimals, 0)) 
            END AS delta_amount
        FROM
            (
                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    pool_id,
                    token,
                    COALESCE(delta, INT256 '0') AS amount
                FROM balances_changes

                UNION ALL

                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    pool_id,
                    token,
                    delta AS amount
                FROM
                    swaps_changes

                UNION ALL

                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    pool_id,
                    token,
                    CAST(delta AS int256) AS amount
                FROM managed_changes
            ) b
        LEFT JOIN tokens.erc20 t ON t.contract_address = b.token
        AND blockchain = 'arbitrum'
        LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)
        WHERE date_trunc('day', evt_block_time) >= CURRENT_DATE - interval '2' day