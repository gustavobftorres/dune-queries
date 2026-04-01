-- part of a query repo
-- query name: Yield Fees Collected, computeinvariant call
-- query link: https://dune.com/queries/4187601


WITH data AS(
SELECT
    call_block_number,
    call_block_time,
    call_tx_hash,
    call_tx_index,
    pool_address,
    token_address,
    CAST(balancesLiveScaled18 AS DOUBLE) 
        - CAST(previous_balance AS DOUBLE) - CAST(swapFeeCollected AS DOUBLE) 
        - swap_balance_change
    AS yield_fee_collected
FROM query_4175112)

SELECT 
    *
FROM data
WHERE yield_fee_collected > 0
ORDER BY call_block_number