-- part of a query repo
-- query name: balancer_v2_trades (Dune SQL)
-- query link: https://dune.com/queries/2856297


WITH
    swap_fees AS (
        SELECT
            swaps.poolId,
            swaps.evt_tx_hash,
            swaps.evt_index,
            swaps.evt_block_number,
            bytearray_substring(swaps.poolId, 1, 20) AS contract_address,
            fees.swap_fee_percentage,
            ROW_NUMBER() OVER (PARTITION BY poolId, evt_tx_hash, evt_index ORDER BY fees.block_number DESC, fees.index DESC) AS rn
        FROM balancer_v2_ethereum.Vault_evt_Swap swaps --## INPUT SOURCE/REF
        LEFT JOIN balancer_v2_ethereum.pools_fees fees --## INPUT SOURCE/REF
            ON fees.contract_address = bytearray_substring(swaps.poolId, 1, 20)
            AND ARRAY[fees.block_number] || ARRAY[fees.index] < ARRAY[swaps.evt_block_number] || ARRAY[swaps.evt_index]
        -- {% if is_incremental() %}
        WHERE swaps.evt_block_time >= date_trunc('day', NOW() - interval '7' day)
        -- {% endif %}
    ),
    dexs AS (
        SELECT
            swap.evt_block_number,
            swap.evt_block_time AS block_time,
            '' AS taker,
            '' AS maker,
            swap.amountOut AS token_bought_amount_raw,
            swap.amountIn AS token_sold_amount_raw,
            CAST(NULL as DOUBLE) AS amount_usd,
            swap.tokenOut AS token_bought_address,
            swap.tokenIn AS token_sold_address,
            swap_fees.contract_address AS project_contract_address,
            swap.poolId AS poolId,
            swap_fees.swap_fee_percentage / POWER(10, 18) AS swap_fee,
            swap.evt_tx_hash AS tx_hash,
            '' AS trace_address,
            swap.evt_index
        FROM
            swap_fees
            INNER JOIN balancer_v2_ethereum.Vault_evt_Swap swap --## INPUT SOURCE/REF
                ON swap.evt_block_number = swap_fees.evt_block_number
                AND swap.evt_tx_hash = swap_fees.evt_tx_hash
                AND swap.evt_index = swap_fees.evt_index
        WHERE
            swap.tokenIn <> swap_fees.contract_address
            AND swap.tokenOut <> swap_fees.contract_address
            AND swap_fees.rn = 1
    ),
    bpa AS (
        SELECT
            dexs.evt_block_number,
            dexs.tx_hash,
            dexs.evt_index,
            bpt_prices.contract_address,
            dexs.block_time,
            MAX(bpt_prices.hour) AS bpa_max_block_time
        FROM
            dexs
            LEFT JOIN balancer_v2_ethereum.bpt_prices bpt_prices --## INPUT SOURCE/REF
                ON bpt_prices.contract_address = CAST(dexs.token_bought_address AS VARCHAR)
                AND bpt_prices.hour <= dexs.block_time
                -- {% if not is_incremental() %}
                AND bpt_prices.hour >= TIMESTAMP '2021-04-20'
                -- {% endif %}
                -- {% if is_incremental() %}
                AND bpt_prices.hour >= DATE_TRUNC('day', NOW() - interval '7' day)
                -- {% endif %}
        GROUP BY 1, 2, 3, 4, 5
    ),
    bpb AS (
        SELECT
            dexs.evt_block_number,
            dexs.tx_hash,
            dexs.evt_index,
            bpt_prices.contract_address,
            dexs.block_time,
            MAX(bpt_prices.hour) AS bpb_max_block_time
        FROM
            dexs
            LEFT JOIN balancer_v2_ethereum.bpt_prices bpt_prices --## INPUT SOURCE/REF
                ON bpt_prices.contract_address = CAST(dexs.token_sold_address AS VARCHAR)
                AND bpt_prices.hour <= dexs.block_time
                -- {% if not is_incremental() %}
                AND bpt_prices.hour >= TIMESTAMP '2021-04-20'
                -- {% endif %}
                -- {% if is_incremental() %}
                AND bpt_prices.hour >= DATE_TRUNC('day', NOW() - interval '7' day)
                -- {% endif %}
        GROUP BY 1, 2, 3, 4, 5
    )

SELECT
    'ethereum' AS blockchain,
    'balancer' AS project,
    '2' AS version,
    DATE_TRUNC('DAY', dexs.block_time) AS block_date,
    dexs.block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    CASE WHEN LOWER(erc20a.symbol) > LOWER(erc20b.symbol) THEN
        CONCAT(erc20b.symbol, '-', erc20a.symbol)
    ELSE
        CONCAT(erc20a.symbol, '-', erc20b.symbol)
    END AS token_pair,
    dexs.token_bought_amount_raw / POWER(10, COALESCE(erc20a.decimals, 18)) AS token_bought_amount,
    dexs.token_sold_amount_raw / POWER(10, COALESCE(erc20b.decimals, 18)) AS token_sold_amount,
    CAST(dexs.token_bought_amount_raw AS DOUBLE) AS token_bought_amount_raw,
    CAST(dexs.token_sold_amount_raw AS DOUBLE) AS token_sold_amount_raw,
    COALESCE(
        dexs.amount_usd,
        dexs.token_bought_amount_raw / POWER(10, p_bought.decimals) * p_bought.price,
        dexs.token_sold_amount_raw / POWER(10, p_sold.decimals) * p_sold.price,
        dexs.token_bought_amount_raw / POWER(10, COALESCE(erc20a.decimals, 18)) * bpa_bpt_prices.median_price,
        dexs.token_sold_amount_raw / POWER(10, COALESCE(erc20b.decimals, 18))  * bpb_bpt_prices.median_price
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker AS taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.poolId,
    dexs.swap_fee,
    dexs.tx_hash,
    tx."from" AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
FROM
    dexs
    INNER JOIN ethereum.transactions tx --## INPUT SOURCE/REF
        ON tx.hash = dexs.tx_hash
        -- {% if not is_incremental() %}
        AND tx.block_time >= TIMESTAMP '2021-04-20'
        -- {% endif %}
        -- {% if is_incremental() %}
        AND tx.block_time >= DATE_TRUNC('day', NOW() - interval '7' day)
        -- {% endif %}
    LEFT JOIN tokens.erc20 erc20a --## INPUT SOURCE/REF
        ON erc20a.contract_address = dexs.token_bought_address
        AND erc20a.blockchain = 'ethereum'
    LEFT JOIN tokens.erc20 erc20b --## INPUT SOURCE/REF
        ON erc20b.contract_address = dexs.token_sold_address
        AND erc20b.blockchain = 'ethereum'
    LEFT JOIN prices.usd p_bought --## INPUT SOURCE/REF
        ON p_bought.minute = DATE_TRUNC('minute', dexs.block_time)
        AND p_bought.contract_address = dexs.token_bought_address
        AND p_bought.blockchain = 'ethereum'
        -- {% if not is_incremental() %}
        AND p_bought.minute >= TIMESTAMP '2021-04-20'
        -- {% endif %}
        -- {% if is_incremental() %}
        AND p_bought.minute >= DATE_TRUNC('day', NOW() - interval '7' day)
        -- {% endif %}
    LEFT JOIN prices.usd p_sold --## INPUT SOURCE/REF
        ON p_sold.minute = DATE_TRUNC('minute', dexs.block_time)
        AND p_sold.contract_address = dexs.token_sold_address
        AND p_sold.blockchain = 'ethereum'
        -- {% if not is_incremental() %}
        AND p_sold.minute >= TIMESTAMP '2021-04-20'
        -- {% endif %}
        -- {% if is_incremental() %}
        AND p_sold.minute >= DATE_TRUNC('day', NOW() - interval '7' day)
        -- {% endif %}
    INNER JOIN bpa
        ON bpa.evt_block_number = dexs.evt_block_number
        AND bpa.tx_hash = dexs.tx_hash
        AND bpa.evt_index = dexs.evt_index
    LEFT JOIN balancer_v2_ethereum.bpt_prices bpa_bpt_prices --## INPUT SOURCE/REF
        ON bpa_bpt_prices.contract_address = bpa.contract_address
        AND bpa_bpt_prices.hour = bpa.bpa_max_block_time
        -- {% if not is_incremental() %}
        AND bpa_bpt_prices.hour >= TIMESTAMP '2021-04-20'
        -- {% endif %}
        -- {% if is_incremental() %}
        AND bpa_bpt_prices.hour >= DATE_TRUNC('day', NOW() - interval '7' day)
        -- {% endif %}
    INNER JOIN bpb
        ON bpb.evt_block_number = dexs.evt_block_number
        AND bpb.tx_hash = dexs.tx_hash
        AND bpb.evt_index = dexs.evt_index
    LEFT JOIN balancer_v2_ethereum.bpt_prices bpb_bpt_prices --## INPUT SOURCE/REF
        ON bpb_bpt_prices.contract_address = bpb.contract_address
        AND bpb_bpt_prices.hour = bpb.bpb_max_block_time
        -- {% if not is_incremental() %}
        AND bpa_bpt_prices.hour >= TIMESTAMP '2021-04-20'
        -- {% endif %}
        -- {% if is_incremental() %}
        AND bpa_bpt_prices.hour >= DATE_TRUNC('day', NOW() - interval '7' day)
        -- {% endif %}

WHERE dexs.tx_hash = {{tx_hash}} -- Not apart of the Spell