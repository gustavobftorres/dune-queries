-- part of a query repo
-- query name: Balancer Exchange Gas Costs (Dune SQL)
-- query link: https://dune.com/queries/2829254


WITH prices AS (
    SELECT date_trunc('day', minute) AS day, AVG(price) AS price 
    FROM prices.usd
    GROUP BY 1
)

SELECT date_trunc('day', s.block_time) AS day,
        MIN(CAST(t.gas_price AS DECIMAL) * t.gas_used * 1e-18 * p.price) AS "Min",
        MAX(CAST(t.gas_price AS DECIMAL) * t.gas_used * 1e-18 * p.price) AS "Max",
        APPROX_PERCENTILE(CAST(t.gas_price AS DECIMAL) * t.gas_used * 1e-18 * p.price,0.5) AS "Median",
        CAST(MIN(t.gas_used) AS BIGINT) AS "Min_gas",
        CAST(MAX(t.gas_used) AS BIGINT) AS "Max_gas",
        CAST(APPROX_PERCENTILE(t.gas_used,0.5) AS BIGINT) AS "Median_gas"
FROM dex.trades s
LEFT JOIN {{Blockchain}}.transactions t ON t.hash = s.tx_hash AND t.block_time > now() - interval '14' day AND s.blockchain = '{{Blockchain}}'
LEFT JOIN prices p ON p.day = date_trunc('day', s.block_time)
WHERE s.block_time > now() - interval '14' day
AND project = 'balancer'
GROUP BY 1