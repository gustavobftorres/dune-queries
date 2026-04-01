-- part of a query repo
-- query name: Balancer vs Uniswap Volume by Trade Size
-- query link: https://dune.com/queries/6681596


WITH aave_weth_trades AS (
    SELECT
        CASE
            WHEN amount_usd < 1000 THEN '$0-1K'
            WHEN amount_usd < 5000 THEN '$1K-5K'
            WHEN amount_usd < 10000 THEN '$5K-10K'
            WHEN amount_usd < 50000 THEN '$10K-50K'
            WHEN amount_usd < 100000 THEN '$50K-100K'
            ELSE '$100K+'
        END as size_bucket,
        amount_usd,
        project
    FROM dex.trades
    WHERE blockchain = 'ethereum'
        AND block_time >= TIMESTAMP '2026-02-01'
        AND token_pair = 'AAVE-WETH'
        AND project IN ('balancer', 'uniswap')
    ORDER BY amount_usd
)
SELECT
    size_bucket,
    SUM(amount_usd) as total_volume,
    SUM(CASE WHEN project = 'balancer' THEN amount_usd ELSE 0 END) as balancer_volume,
    SUM(CASE WHEN project = 'uniswap' THEN amount_usd ELSE 0 END) as uniswap_volume,
    SUM(CASE WHEN project = 'balancer' THEN amount_usd ELSE 0 END) / SUM(amount_usd) as balancer_share,
    SUM(CASE WHEN project = 'uniswap' THEN amount_usd ELSE 0 END) / SUM(amount_usd) as uniswap_share
FROM aave_weth_trades
GROUP BY 1
ORDER BY MIN(amount_usd)
