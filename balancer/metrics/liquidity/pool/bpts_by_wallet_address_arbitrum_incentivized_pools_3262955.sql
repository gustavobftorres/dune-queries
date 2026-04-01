-- part of a query repo
-- query name: BPTs by Wallet Address (Arbitrum Incentivized Pools)
-- query link: https://dune.com/queries/3262955


WITH 
    -- has information regarding all Pools/Gauges incentivized in the latest round
    arbitrum_incentives AS (
        SELECT recipientgaugeaddr as gauge_address, pooladdress AS pool_address, symbol
        FROM dune.balancer.dataset_arbitrum_incentives_latest
    ),

    -- wallets that hold BPTs and are not eligible for rewards
    liquidity_providers AS (
        SELECT pool_address, a.symbol, wallet_address, amount_raw / 1e18 AS amount
        FROM balances_arbitrum.erc20_latest b
        JOIN arbitrum_incentives a
        ON b.token_address = a.pool_address
        AND wallet_address != 0xba12222222228d8ba445958a75a0704d566bf2c8
        AND wallet_address NOT IN (SELECT gauge_address FROM arbitrum_incentives)
        AND amount_raw > 0
    ),
    
    -- wallets that staked the BPT in the Gauge to receive rewards
    -- these are LPs but can't be tracked by CTE above
    gauge_stakers AS (
        SELECT pool_address, a.symbol, wallet_address, amount_raw / 1e18 AS amount
        FROM balances_arbitrum.erc20_latest b
        JOIN arbitrum_incentives a
        ON b.token_address = a.gauge_address
        AND amount_raw > 0
    ),
    
    -- contains every wallet that holds BPTs directly or indirectly
    bpt_holders AS (
        SELECT * FROM liquidity_providers
        UNION ALL
        SELECT * FROM gauge_stakers
    )

-- we're doing this because it's possible someone has BPT staked and unstaked
-- note there are still contracts holding BPTs or Gauge Deposit Tokens (e.g. Aura and Radiant)
SELECT pool_address, symbol, wallet_address, SUM(amount) AS amount
FROM bpt_holders
GROUP BY 1, 2, 3
ORDER BY 1, 4 DESC
