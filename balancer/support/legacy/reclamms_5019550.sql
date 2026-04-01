-- part of a query repo
-- query name: ReCLAMMs
-- query link: https://dune.com/queries/5019550


WITH reclamms AS (
    SELECT 'base' AS chain, pool
    FROM balancer_v3_base.vault_evt_poolregistered
    WHERE factory IN (
    0x84813aa3e079a665c0b80f944427ee83cba63617,
    0x7fA49Df302a98223d98D115fc4FCD275576f6faA,
    0xa3b370092aeb56770b23315252ab5e16dacbf62b
)

),
swap_fees AS (
    SELECT
        block_date,
        blockchain,
        project_contract_address,
        SUM(amount_usd * swap_fee) AS swap_fee_usd
    FROM balancer.trades d
    JOIN reclamms r
    ON r.chain = d.blockchain
    AND d.project_contract_address = r.pool
    AND d.version = '3'
    GROUP BY 1, 2, 3
),
-- Aggregate fees to one row per pool
fees_totals AS (
    SELECT
        blockchain,
        project_contract_address,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '7' day THEN swap_fee_usd ELSE 0 END) AS fees_7d,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '30' day THEN swap_fee_usd ELSE 0 END) AS fees_30d,
        SUM(swap_fee_usd) AS fees_all_time
    FROM swap_fees
    GROUP BY 1, 2
)
SELECT 
    m.blockchain,
    q.pool,
    m.pool_symbol,
    MAX(CASE WHEN m.block_date = (SELECT MAX(day) FROM balancer.liquidity) THEN tvl_usd ELSE NULL END) AS tvl_usd,
    SUM(CASE WHEN m.block_date = (SELECT MAX(day) FROM balancer.liquidity) THEN swap_amount_usd ELSE 0 END) AS today_volume,
    SUM(CASE WHEN m.block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '7' day THEN swap_amount_usd ELSE 0 END) AS volume_7d,
    SUM(CASE WHEN m.block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '30' day THEN swap_amount_usd ELSE 0 END) AS volume_30d,
    SUM(swap_amount_usd) AS volume_all_time,
    COALESCE(f.fees_7d, 0) AS fees_7d,
    COALESCE(f.fees_30d, 0) AS fees_30d,
    COALESCE(f.fees_all_time, 0) AS fees_all_time
FROM balancer.pools_metrics_daily m
JOIN reclamms q
ON q.chain = m.blockchain
AND q.pool = m.project_contract_address
LEFT JOIN fees_totals f 
ON f.blockchain = m.blockchain
AND f.project_contract_address = m.project_contract_address
GROUP BY 1, 2, 3, f.fees_7d, f.fees_30d, f.fees_all_time
ORDER BY 4 DESC
