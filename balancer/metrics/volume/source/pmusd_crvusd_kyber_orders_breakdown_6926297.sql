-- part of a query repo
-- query name: pmusd/crvusd kyber orders breakdown
-- query link: https://dune.com/queries/6926297


WITH kyber_curve_txs AS (
    -- 1. Isolate KyberSwap aggregator trades that routed through this specific Curve pool.
    -- Using DISTINCT on the aggregator trade attributes prevents duplication if the 
    -- router hit the same Curve pool multiple times in a complex multi-hop trade.
    SELECT DISTINCT 
        agg.tx_hash,
        agg.token_pair,
        agg.amount_usd
    FROM dex_aggregator.trades agg
    INNER JOIN dex.trades dex 
        ON agg.tx_hash = dex.tx_hash
    WHERE agg.blockchain = 'ethereum'
        AND agg.project = 'kyberswap' 
        AND dex.blockchain = 'ethereum'
        AND dex.project_contract_address = 0xecb0f0d68c19bdaadaebe24f6752a4db34e2c2cb -- curve pool 
)

SELECT 
    token_pair,
    COUNT(tx_hash) as total_trades,
    SUM(amount_usd) as total_volume_usd,
    AVG(amount_usd) as avg_trade_size_usd,
    APPROX_PERCENTILE(amount_usd, 0.5) as median_trade_size_usd,
    
    -- Trade Count Distribution
    COUNT(CASE WHEN amount_usd < 1000 THEN 1 END) as count_retail_under_1k,
    COUNT(CASE WHEN amount_usd >= 1000 AND amount_usd < 10000 THEN 1 END) as count_1k_to_10k,
    COUNT(CASE WHEN amount_usd >= 10000 AND amount_usd < 100000 THEN 1 END) as count_10k_to_100k,
    COUNT(CASE WHEN amount_usd >= 100000 THEN 1 END) as count_whale_over_100k,
    
    -- Volume Distribution Percentage
    SUM(CASE WHEN amount_usd < 1000 THEN amount_usd ELSE 0 END) / SUM(amount_usd) as pct_vol_under_1k,
    SUM(CASE WHEN amount_usd >= 100000 THEN amount_usd ELSE 0 END) / SUM(amount_usd) as pct_vol_over_100k

FROM kyber_curve_txs
GROUP BY 1
ORDER BY total_volume_usd DESC
