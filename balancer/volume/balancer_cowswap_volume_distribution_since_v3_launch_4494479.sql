-- part of a query repo
-- query name: balancer <> cowswap volume distribution (since v3 launch)
-- query link: https://dune.com/queries/4494479


SELECT 
    concat(project, '-v', version) AS project,
    avg(amount_usd) as avg_volume,
    approx_percentile(amount_usd, 0.25) as p25_volume,
    approx_percentile(amount_usd, 0.50) as p50_volume,
    approx_percentile(amount_usd, 0.75) as p75_volume,
    approx_percentile(amount_usd, 0.90) as p90_volume,
    approx_percentile(amount_usd, 0.95) as p95_volume,
    count(*) as num_trades,
    min(amount_usd) as min_volume,
    max(amount_usd) as max_volume
FROM dex.trades
WHERE blockchain = 'ethereum'
AND project IN ('balancer')
AND tx_to = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41
-- AND project_contract_address = 0xc4Ce391d82D164c166dF9c8336DDF84206b2F812
AND block_date >= timestamp '2024-12-12'
GROUP BY 1