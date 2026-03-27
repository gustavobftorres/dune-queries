-- part of a query repo
-- query name: base_uniswap_router_priority_fees
-- query link: https://dune.com/queries/4242214


WITH transactions AS (
    SELECT
        block_date,
        block_time,
        block_number,
        tx_hash,
        TRY(tx_fee_breakdown['priority_fee']) AS priority_fee,
        TRY(tx_fee_breakdown_usd['priority_fee']) AS priority_fee_usd
    FROM
        gas_base.fees
    WHERE 
        (('{{uni_version}}' = 'V1' AND tx_to IN (0xec8b0f7ffe3ae75d7ffab09429e3675bb63503e4, 0x198ef79f1f515f02dfe9e3115ed9fc07183f02fc, 0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad)) OR
         ('{{uni_version}}' = 'V2' AND tx_to IN (0x4cf76043b3f97ba06917cbd90f9e3a2aac1b306e, 0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24)) OR
         ('{{uni_version}}' = 'V3' AND tx_to IN (0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad)) OR
         ('{{uni_version}}' = 'All' AND tx_to IN (
            0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad,
            0x4cf76043b3f97ba06917cbd90f9e3a2aac1b306e, 0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24, 
            0xec8b0f7ffe3ae75d7ffab09429e3675bb63503e4, 0x198ef79f1f515f02dfe9e3115ed9fc07183f02fc, 
            0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad))
        )
        AND block_time >= TIMESTAMP '{{start_time}}'
        AND block_time <= TIMESTAMP '{{end_time}}'
)

SELECT
    CASE WHEN '{{aggregation}}' = 'date'
    THEN block_date
    WHEN '{{aggregation}}' = 'time'
    THEN block_time   
    END AS time_param,
    APPROX_PERCENTILE(priority_fee, 0.5) AS median_priority_fee,
    APPROX_PERCENTILE(priority_fee_usd, 0.5) AS median_priority_fee_usd,
    MAX(priority_fee) AS max_priority_fee,
    MAX(priority_fee_usd) AS max_priority_fee_usd,
    MIN(priority_fee) AS min_priority_fee,
    MIN(priority_fee_usd) AS min_priority_fee_usd,
    SUM(priority_fee) AS total_priority_fee,
    SUM(priority_fee_usd) AS total_priority_fee_usd
FROM transactions
WHERE priority_fee IS NOT NULL
GROUP BY 1 
ORDER BY 1 DESC