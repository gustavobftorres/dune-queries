-- part of a query repo
-- query name: OP_uniswap_router_priority_fees
-- query link: https://dune.com/queries/4242041


WITH transactions AS (
    SELECT
        block_date,
        block_time,
        block_number,
        tx_hash,
        TRY(tx_fee_breakdown['priority_fee']) AS priority_fee,
        TRY(tx_fee_breakdown_usd['priority_fee']) AS priority_fee_usd
    FROM
        gas_optimism.fees
    WHERE 
        (('{{uni_version}}' = 'V1' AND tx_to IN (0xec8b0f7ffe3ae75d7ffab09429e3675bb63503e4, 0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad, 0xcb1355ff08ab38bbce60111f1bb2b784be25d7e8)) OR
         ('{{uni_version}}' = 'V2' AND tx_to IN (0xf1d7cc64fb4452f05c498126312ebe29f30fbcf9, 0x4a7b5da61326a6379179b40d00f57e5bbdc962c2)) OR
         ('{{uni_version}}' = 'V3' AND tx_to IN (0xfccf1af487be1a1bb663a61334ae0c4c93bbce21, 0xb555edf5dcf85f42ceef1f3630a52a108e55a654)) OR
         ('{{uni_version}}' = 'All' AND tx_to IN (
            0xfccf1af487be1a1bb663a61334ae0c4c93bbce21, 0xb555edf5dcf85f42ceef1f3630a52a108e55a654, 
            0xf1d7cc64fb4452f05c498126312ebe29f30fbcf9, 0x4a7b5da61326a6379179b40d00f57e5bbdc962c2, 
            0xec8b0f7ffe3ae75d7ffab09429e3675bb63503e4, 0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad, 
            0xcb1355ff08ab38bbce60111f1bb2b784be25d7e8))
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
    APPROX_PERCENTILE(priority_fee, 0.5) / 1e9 AS median_priority_fee,
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