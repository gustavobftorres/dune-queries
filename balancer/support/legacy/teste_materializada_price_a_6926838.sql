-- part of a query repo
-- query name: teste materializada price a
-- query link: https://dune.com/queries/6926838


WITH prev AS (
    SELECT * FROM TABLE(previous.query.result(
        schema => DESCRIPTOR(
            minute TIMESTAMP(3),
            price DOUBLE
        )
    ))
),
checkpoint AS (
    SELECT COALESCE(MAX(minute), now() - INTERVAL '2' DAY) - INTERVAL '1' HOUR AS cutoff
    FROM prev
),
window AS (
    SELECT
        (SELECT cutoff FROM checkpoint) AS start_time,
        LEAST(
            (SELECT cutoff FROM checkpoint) + INTERVAL '30' DAY,
            now()
        ) AS end_time
)

SELECT minute, price
FROM prev
WHERE minute < (SELECT start_time FROM window)

UNION ALL

SELECT
    date_trunc('minute', "timestamp" - interval '5' minute) AS minute,
    AVG(CAST(price as DOUBLE)) as price
FROM prices.minute
WHERE
    blockchain = 'arbitrum'
    AND contract_address = 0x0b2b2b2076d95dda7817e785989fe353fe955ef9
    AND "timestamp" >= (SELECT start_time FROM window)
    AND "timestamp" < (SELECT end_time FROM window)
GROUP BY 1
ORDER BY minute
