-- part of a query repo
-- query name: teste materializada price b
-- query link: https://dune.com/queries/6926069


WITH prev AS (
    SELECT * FROM TABLE(previous.query.result(
        schema => DESCRIPTOR(
            minute TIMESTAMP(3),
            price DOUBLE
        )
    ))
),
checkpoint AS (
    SELECT COALESCE(MAX(minute), TIMESTAMP '2025-09-23 00:00:00') - INTERVAL '1' HOUR AS cutoff
    FROM prev
),
-- Limita a janela máxima de cada execução (ex: 30 dias por vez)
window AS (
    SELECT
        (SELECT cutoff FROM checkpoint) AS start_time,
        LEAST(
            (SELECT cutoff FROM checkpoint) + INTERVAL '30' DAY,
            now()
        ) AS end_time
)

-- Mantém histórico já processado
SELECT minute, price
FROM prev
WHERE minute < (SELECT start_time FROM window)

UNION ALL

-- Processa apenas a janela atual (máximo 30 dias)
SELECT
    date_trunc('minute', "timestamp" - interval '5' minute) AS minute,
    AVG(CAST(price as DOUBLE)) as price
FROM prices.minute
WHERE
    blockchain = 'ethereum'
    AND contract_address = 0xdac17f958d2ee523a2206206994597c13d831ec7
    AND "timestamp" >= (SELECT start_time FROM window)
    AND "timestamp" < (SELECT end_time FROM window)
GROUP BY 1

ORDER BY minute
