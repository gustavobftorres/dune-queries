-- part of a query repo
-- query name: Daily Gas Costs, Last 14 Days
-- query link: https://dune.com/queries/2832498


WITH prices AS (
    SELECT date_trunc('day', minute) AS day, AVG(price) AS price, blockchain
    FROM prices.usd
    GROUP BY 1,3
),
arb_transactions AS(
SELECT *, 'arbitrum' as blockchain
FROM arbitrum.transactions
),
av_transactions AS(
SELECT *, 'avalanche_c' as blockchain
FROM avalanche_c.transactions
),
t_transactions AS(
SELECT *, 'ethereum' as blockchain
FROM ethereum.transactions
),
g_transactions AS(
SELECT *, 'gnosis' as blockchain
FROM gnosis.transactions
),
pol_transactions AS(
SELECT *, 'polygon' as blockchain
FROM polygon.transactions
),
o_transactions AS(
SELECT *, 'optimism' as blockchain
FROM optimism.transactions
),
z_transactions AS(
SELECT *, 'zkevm' as blockchain
FROM zkevm.transactions
),
b_transactions AS(
SELECT *, 'base' as blockchain
FROM base.transactions
)

SELECT  CAST(date_trunc('day', s.block_time) as DATE) AS day,
        s.blockchain,
        CASE WHEN s.blockchain = 'arbitrum' THEN MIN(CAST(arb.gas_price as double) * CAST(arb.gas_used as double) * 1e-18 * p.price) 
        WHEN s.blockchain = 'avalanche_c' THEN MIN(CAST(av.gas_price as double) * CAST(av.gas_used as double) * 1e-18 * p.price)
        WHEN s.blockchain = 'base' THEN MIN(CAST(b.gas_price as double) * CAST(b.gas_used as double) * 1e-18 * p.price)
        WHEN s.blockchain = 'ethereum' THEN MIN(CAST(t.gas_price as double) * CAST(t.gas_used as double) * 1e-18 * p.price)
        WHEN s.blockchain = 'gnosis' THEN MIN(CAST(g.gas_price as double) * CAST(g.gas_used as double) * 1e-18 * p.price)
        WHEN s.blockchain = 'polygon' THEN MIN(CAST(pol.gas_price as double) * CAST(pol.gas_used as double) * 1e-18 * p.price)
        WHEN s.blockchain = 'optimism' THEN MIN(CAST(o.gas_price as double) * CAST(o.gas_used as double) * 1e-18 * p.price)
        WHEN s.blockchain = 'zkevm' THEN MIN(CAST(o.gas_price as double) * CAST(o.gas_used as double) * 1e-18 * p.price)
        END AS "Min",
        CASE WHEN s.blockchain = 'arbitrum' THEN APPROX_PERCENTILE(CAST(arb.gas_price as double) * CAST(arb.gas_used as double) * 1e-18 * p.price,0.5) 
        WHEN s.blockchain = 'avalanche_c' THEN APPROX_PERCENTILE(CAST(av.gas_price as double) * CAST(av.gas_used as double) * 1e-18 * p.price,0.5)
        WHEN s.blockchain = 'base' THEN APPROX_PERCENTILE(CAST(b.gas_price as double) * CAST(b.gas_used as double) * 1e-18 * p.price,0.5)
        WHEN s.blockchain = 'ethereum' THEN APPROX_PERCENTILE(CAST(t.gas_price as double) * CAST(t.gas_used as double) * 1e-18 * p.price,0.5)
        WHEN s.blockchain = 'gnosis' THEN APPROX_PERCENTILE(CAST(g.gas_price as double) * CAST(g.gas_used as double) * 1e-18 * p.price,0.5)
        WHEN s.blockchain = 'polygon' THEN APPROX_PERCENTILE(CAST(pol.gas_price as double) * CAST(pol.gas_used as double) * 1e-18 * p.price,0.5)
        WHEN s.blockchain = 'optimism' THEN APPROX_PERCENTILE(CAST(o.gas_price as double) * CAST(o.gas_used as double) * 1e-18 * p.price,0.5)
        WHEN s.blockchain = 'zkevm' THEN APPROX_PERCENTILE(CAST(o.gas_price as double) * CAST(o.gas_used as double) * 1e-18 * p.price,0.5)
        END AS "Median",
        CASE WHEN s.blockchain = 'arbitrum' THEN MAX(CAST(arb.gas_price as double) * CAST(arb.gas_used as double) * 1e-18 * p.price) 
        WHEN s.blockchain = 'avalanche_c' THEN MAX(CAST(av.gas_price as double) * CAST(av.gas_used as double) * 1e-18 * p.price)
        WHEN s.blockchain = 'base' THEN MAX(CAST(b.gas_price as double) * CAST(b.gas_used as double) * 1e-18 * p.price)
        WHEN s.blockchain = 'ethereum' THEN MAX(CAST(t.gas_price as double) * CAST(t.gas_used as double) * 1e-18 * p.price)
        WHEN s.blockchain = 'gnosis' THEN MAX(CAST(g.gas_price as double) * CAST(g.gas_used as double) * 1e-18 * p.price)
        WHEN s.blockchain = 'polygon' THEN MAX(CAST(pol.gas_price as double) * CAST(pol.gas_used as double) * 1e-18 * p.price)
        WHEN s.blockchain = 'optimism' THEN MAX(CAST(o.gas_price as double) * CAST(o.gas_used as double) * 1e-18 * p.price)
        WHEN s.blockchain = 'zkevm' THEN MAX(CAST(o.gas_price as double) * CAST(o.gas_used as double) * 1e-18 * p.price)
        END AS "Max",
        CASE WHEN s.blockchain = 'arbitrum' THEN MIN(CAST(arb.gas_used as double) ) 
        WHEN s.blockchain = 'avalanche_c' THEN MIN(CAST(av.gas_used as double))
        WHEN s.blockchain = 'base' THEN MIN(CAST(b.gas_used as double))
        WHEN s.blockchain = 'ethereum' THEN MIN(CAST(t.gas_used as double))
        WHEN s.blockchain = 'gnosis' THEN MIN(CAST(g.gas_used as double))
        WHEN s.blockchain = 'polygon' THEN MIN(CAST(pol.gas_used as double))
        WHEN s.blockchain = 'optimism' THEN MIN(CAST(o.gas_used as double))
        WHEN s.blockchain = 'zkevm' THEN MIN(CAST(o.gas_used as double))
        END AS "Min_gas_used",
        CASE WHEN s.blockchain = 'arbitrum' THEN APPROX_PERCENTILE(CAST(arb.gas_used as double),0.5)
        WHEN s.blockchain = 'avalanche_c' THEN APPROX_PERCENTILE(CAST(av.gas_used as double),0.5)
        WHEN s.blockchain = 'base' THEN APPROX_PERCENTILE(CAST(b.gas_used as double),0.5)
        WHEN s.blockchain = 'ethereum' THEN APPROX_PERCENTILE(CAST(t.gas_used as double),0.5) 
        WHEN s.blockchain = 'gnosis' THEN APPROX_PERCENTILE(CAST(g.gas_used as double),0.5) 
        WHEN s.blockchain = 'polygon' THEN APPROX_PERCENTILE(CAST(pol.gas_used as double),0.5) 
        WHEN s.blockchain = 'optimism' THEN APPROX_PERCENTILE(CAST(o.gas_used as double),0.5) 
        WHEN s.blockchain = 'zkevm' THEN APPROX_PERCENTILE(CAST(o.gas_used as double),0.5) 
        END AS "Median_gas_used",
        CASE WHEN s.blockchain = 'arbitrum' THEN MAX(CAST(arb.gas_used as double)) 
        WHEN s.blockchain = 'avalanche_c' THEN MAX(CAST(av.gas_used as double))
        WHEN s.blockchain = 'base' THEN MAX(CAST(b.gas_used as double))
        WHEN s.blockchain = 'ethereum' THEN MAX(CAST(t.gas_used as double))
        WHEN s.blockchain = 'gnosis' THEN MAX(CAST(g.gas_used as double))
        WHEN s.blockchain = 'polygon' THEN MAX(CAST(pol.gas_used as double))
        WHEN s.blockchain = 'optimism' THEN MAX(CAST(o.gas_used as double))
        WHEN s.blockchain = 'zkevm' THEN MAX(CAST(o.gas_used as double))
        END AS "Max_gas_used"
FROM dex.trades s
LEFT JOIN arb_transactions arb ON arb.hash = s.tx_hash AND arb.block_time > now() - interval '14' day AND arb.blockchain = s.blockchain
LEFT JOIN av_transactions av ON av.hash = s.tx_hash AND av.block_time > now() - interval '14' day AND av.blockchain = s.blockchain
LEFT JOIN t_transactions t ON t.hash = s.tx_hash AND t.block_time > now() - interval '14' day AND t.blockchain = s.blockchain
LEFT JOIN g_transactions g ON g.hash = s.tx_hash AND g.block_time > now() - interval '14' day AND g.blockchain = s.blockchain
LEFT JOIN pol_transactions pol ON pol.hash = s.tx_hash AND pol.block_time > now() - interval '14' day AND pol.blockchain = s.blockchain
LEFT JOIN o_transactions o ON o.hash = s.tx_hash AND o.block_time > now() - interval '14' day AND o.blockchain = s.blockchain
LEFT JOIN b_transactions b ON b.hash = s.tx_hash AND b.block_time > now() - interval '14' day AND b.blockchain = s.blockchain
LEFT JOIN z_transactions z ON z.hash = s.tx_hash AND z.block_time > now() - interval '14' day AND z.blockchain = s.blockchain
LEFT JOIN prices p ON p.day = date_trunc('day', s.block_time) AND p.blockchain = s.blockchain
WHERE s.block_time > now() - interval '14' day
AND project = 'balancer'
AND tx_to = 0xba12222222228d8ba445958a75a0704d566bf2c8
GROUP BY 1,2
ORDER BY 1 DESC, 4 DESC