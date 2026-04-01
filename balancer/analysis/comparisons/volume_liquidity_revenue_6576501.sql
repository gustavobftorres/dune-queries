-- part of a query repo
-- query name: Volume, Liquidity, revenue
-- query link: https://dune.com/queries/6576501


-- Fluid DEX Liquidity Pools Summary
WITH latest_tvl AS (
    SELECT 
        blockchain,
        project,
        version,
        dex,
        token0,
        token1,
        token0_symbol,
        token1_symbol,
        token0_balance_usd,
        token1_balance_usd,
        borrow_token0_balance_usd,
        borrow_token1_balance_usd,
        (token0_balance_usd + token1_balance_usd) as supplied_usd,
        (COALESCE(borrow_token0_balance_usd, 0) + COALESCE(borrow_token1_balance_usd, 0)) as borrowed_usd
    FROM fluid.tvl_daily
    WHERE block_date = (SELECT MAX(block_date) FROM fluid.tvl_daily)
),
pool_info AS (
    SELECT 
        blockchain,
        dex,
        supply_token_symbol,
        borrow_token_symbol,
        isSmartCol,
        isSmartDebt,
        fee,
        revenue_cut,
        CASE 
            WHEN isSmartCol AND isSmartDebt THEN 'Smart-Smart'
            WHEN isSmartCol AND NOT isSmartDebt THEN 'Smart-Normal'
            WHEN NOT isSmartCol AND isSmartDebt THEN 'Normal-Smart'
            ELSE 'Normal-Normal'
        END as collateral_debt_type
    FROM fluid.pools
),
volume_data AS (
    SELECT 
        project_contract_address as dex,
        blockchain,
        SUM(CASE WHEN block_time >= NOW() - INTERVAL '1' DAY THEN amount_usd ELSE 0 END) as volume_1d,
        SUM(CASE WHEN block_time >= NOW() - INTERVAL '7' DAY THEN amount_usd ELSE 0 END) as volume_7d,
        SUM(CASE WHEN block_time >= NOW() - INTERVAL '30' DAY THEN amount_usd ELSE 0 END) as volume_30d,
        SUM(CASE WHEN block_time >= NOW() - INTERVAL '60' DAY AND block_time < NOW() - INTERVAL '30' DAY THEN amount_usd ELSE 0 END) as volume_prev_30d
    FROM dex.trades
    WHERE project = 'fluid'
    AND block_time >= NOW() - INTERVAL '60' DAY
    GROUP BY 1, 2
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY (t.supplied_usd + t.borrowed_usd) DESC NULLS LAST) as "#",
    INITCAP(t.blockchain) as "Blockchain",
    CONCAT(t.token0_symbol, '-', t.token1_symbol) as "Pool",
    COALESCE(p.collateral_debt_type, 'Smart-Normal') as "Collateral-Debt",
    CONCAT(FORMAT('%.3f', COALESCE(p.fee, 0.0001) * 100), '%') as "Trading Fee",
    CONCAT(FORMAT('%.0f', COALESCE(p.revenue_cut, 25)), '%') as "Revenue Cut",
    CONCAT('$', FORMAT('%.2f', t.supplied_usd / 1000000), 'm') as "Supplied",
    CONCAT('$', FORMAT('%.2f', t.borrowed_usd / 1000000), 'm') as "Borrowed",
    CONCAT('$', FORMAT('%.2f', (t.supplied_usd + t.borrowed_usd) / 1000000), 'm') as "Liquidity",
    -- Cap. Eff. = 24h Volume / Liquidity
    CONCAT(FORMAT('%.1f', 
        CASE WHEN (t.supplied_usd + t.borrowed_usd) > 0 
        THEN (COALESCE(v.volume_1d, 0) / (t.supplied_usd + t.borrowed_usd)) * 100 
        ELSE 0 END), '%') as "Cap. Eff.",
    CONCAT('$', FORMAT('%.2f', COALESCE(v.volume_1d, 0) / 1000000), 'm') as "Volume 1D",
    CONCAT('$', FORMAT('%.2f', COALESCE(v.volume_7d, 0) / 1000000), 'm') as "Volume 7D", 
    CONCAT('$', FORMAT('%.2f', COALESCE(v.volume_30d, 0) / 1000000), 'm') as "Volume 30D",
    CONCAT(FORMAT('%.1f', 
        CASE WHEN COALESCE(v.volume_prev_30d, 0) > 0 
        THEN ((COALESCE(v.volume_30d, 0) - COALESCE(v.volume_prev_30d, 0)) / v.volume_prev_30d) * 100 
        ELSE 0 END), '%') as "Volume 30D %",
    CONCAT('$', FORMAT('%.2f', COALESCE(v.volume_7d, 0) * COALESCE(p.fee, 0.0001) / 1000), 'k') as "Fees 7D",
    CONCAT('$', FORMAT('%.2f', COALESCE(v.volume_30d, 0) * COALESCE(p.fee, 0.0001) / 1000), 'k') as "Fees 30D",
    CONCAT('$', FORMAT('%.2f', COALESCE(v.volume_7d, 0) * COALESCE(p.fee, 0.0001) * COALESCE(p.revenue_cut, 25) / 100 / 1000), 'k') as "Revenue 7D",
    CONCAT('$', FORMAT('%.2f', COALESCE(v.volume_30d, 0) * COALESCE(p.fee, 0.0001) * COALESCE(p.revenue_cut, 25) / 100 / 1000), 'k') as "Revenue 30D"
FROM latest_tvl t
LEFT JOIN pool_info p ON t.dex = p.dex AND t.blockchain = p.blockchain
LEFT JOIN volume_data v ON t.dex = v.dex AND t.blockchain = v.blockchain
WHERE (t.supplied_usd + t.borrowed_usd) > 1000  -- Filter out dust/empty pools
ORDER BY (t.supplied_usd + t.borrowed_usd) DESC NULLS LAST