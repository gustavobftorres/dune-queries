-- part of a query repo
-- query name: stS Delegation over time
-- query link: https://dune.com/queries/4534665


WITH
operations_1 AS (
    SELECT 
        block_time,
        DATE_TRUNC('day', block_time) AS day,
        tx_hash,
        varbinary_to_int256(topic1) AS validatorId,
        varbinary_to_int256(data) AS amount
    FROM sonic.logs
    WHERE contract_address = 0xe5da20f15420ad15de0fa650600afc998bbe3955
      AND topic0 = 0xdf2a7c5f7a567419f37f5bba40b572a4500cdf7c85f7b18a67c6dba1b94fba3b

    UNION ALL

    SELECT 
        block_time,
        DATE_TRUNC('day', block_time) AS day,
        tx_hash,
        varbinary_to_int256(BYTEARRAY_SUBSTRING(data, 33, 32)) AS validatorId,
        -varbinary_to_int256(BYTEARRAY_SUBSTRING(data, 65, 32)) AS amount
    FROM sonic.logs
    WHERE contract_address = 0xe5da20f15420ad15de0fa650600afc998bbe3955
      AND topic0 = 0x04fcca04f81983ffc61b309cc6d2935c3e78576bed7045f109b779920d0a1455
    ),

    operations AS (
    SELECT
        day,
        validatorId,
        SUM(amount / POWER(10, 18)) OVER (PARTITION BY validatorId ORDER BY day) AS delegated
    FROM operations_1
    WHERE validatorId != 0
    ),

    calendar AS (
        SELECT 
            date_sequence AS day
        FROM unnest(sequence(date('2024-12-15'), date(now()), interval '1' day)) as t(date_sequence)
    )

    SELECT
        c.day,
        j.validatorId,
        j.delegated
    FROM calendar c 
    LEFT JOIN operations j ON c.day = j.day