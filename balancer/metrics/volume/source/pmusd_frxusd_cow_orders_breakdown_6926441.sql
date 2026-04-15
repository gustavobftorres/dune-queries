-- part of a query repo
-- query name: pmusd/frxusd cow orders breakdown
-- query link: https://dune.com/queries/6926441


WITH cow_curve_txs AS (
    -- 1. Isolate the specific CoW settlement transactions that hit this Curve pool
    SELECT DISTINCT tx_hash
    FROM dex.trades
    WHERE blockchain = 'ethereum'
        AND project_contract_address = 0xbf5047039f2980c21eb5692c790bad8a9533b900 -- curve pool 
        AND tx_to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41 -- cow settlement
)

SELECT 
    agg.token_pair,
    COUNT(agg.tx_hash) as total_trades,
    SUM(agg.amount_usd) as total_volume_usd,
    AVG(agg.amount_usd) as avg_trade_size_usd,
    APPROX_PERCENTILE(agg.amount_usd, 0.5) as median_trade_size_usd,
    
    -- Trade Count Distribution
    COUNT(CASE WHEN agg.amount_usd < 1000 THEN 1 END) as count_under_1k,
    COUNT(CASE WHEN agg.amount_usd >= 1000 AND agg.amount_usd < 10000 THEN 1 END) as count_1k_to_10k,
    COUNT(CASE WHEN agg.amount_usd >= 10000 AND agg.amount_usd < 100000 THEN 1 END) as count_10k_to_100k,
    COUNT(CASE WHEN agg.amount_usd >= 100000 THEN 1 END) as count_over_100k,
    
    -- Volume Distribution Percentage
    SUM(CASE WHEN agg.amount_usd < 1000 THEN agg.amount_usd ELSE 0 END) / SUM(agg.amount_usd) as pct_vol_under_1k,
    SUM(CASE WHEN agg.amount_usd >= 100000 THEN agg.amount_usd ELSE 0 END) / SUM(agg.amount_usd) as pct_vol_over_100k

FROM dex_aggregator.trades agg
INNER JOIN cow_curve_txs c 
    ON agg.tx_hash = c.tx_hash
WHERE agg.blockchain = 'ethereum'
GROUP BY 1
ORDER BY total_volume_usd DESC
