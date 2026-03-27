-- part of a query repo
-- query name: Balancer LST Pools
-- query link: https://dune.com/queries/3375874


WITH 
pools AS(
    SELECT * 
    FROM dune.balancer.result_lst_pools
),


volume AS (
    SELECT
        CAST(project_contract_address AS VARCHAR) AS project_contract_address,
        t.blockchain,
        SUM(t.amount_usd) FILTER(WHERE t.block_time >= now() - INTERVAL '24' hour) AS volume_24h,
        SUM(t.amount_usd / median_price_eth) FILTER(WHERE t.block_time >= now() - INTERVAL '24' hour) AS volume_24h_eth,
        SUM(t.amount_usd) FILTER(WHERE t.block_time >= now() - INTERVAL '30' day) AS volume_30d,
        SUM(t.amount_usd / median_price_eth) FILTER(WHERE t.block_time >= now() - INTERVAL '30' day) AS volume_30d_eth,
        SUM(t.amount_usd) AS volume_all_time,
        SUM(t.amount_usd / median_price_eth) AS volume_all_time_eth
    FROM balancer.trades t
    LEFT JOIN pools p ON p.pool_address = t.project_contract_address
        AND p.blockchain = t.blockchain
    LEFT JOIN dune.balancer.result_eth_price p ON t.block_date = p.day
    WHERE
        p.name IS NOT NULL 
    GROUP BY 1, 2
),

fees AS(
    SELECT
    f.pool_address,
    f.blockchain,
    SUM(f.protocol_fee_collected_usd) FILTER(WHERE f.day >= now() - INTERVAL '30' day) AS fees_collected_30d,
    SUM(f.protocol_fee_collected_usd / median_price_eth) FILTER(WHERE f.day >= now() - INTERVAL '30' day) AS fees_collected_30d_eth,
    SUM(f.protocol_fee_collected_usd) AS fees_collected,
    SUM(f.protocol_fee_collected_usd / median_price_eth) AS fees_collected_eth
    FROM balancer.protocol_fee f
    LEFT JOIN pools p ON p.pool_address = f.pool_address AND p.blockchain = f.blockchain
    LEFT JOIN dune.balancer.result_eth_price p ON f.day = p.day
    WHERE
        p.name IS NOT NULL 
    GROUP BY 1, 2
)

    SELECT
        UPPER(SUBSTRING("name", 1, 60)) AS name,
        "poolID",
        CASE
            WHEN SUM(protocol_liquidity_usd) IS NULL THEN 0
            ELSE SUM(protocol_liquidity_usd)
        END AS TVL,
        CASE
            WHEN SUM(protocol_liquidity_usd / median_price_eth) IS NULL THEN 0
            ELSE SUM(protocol_liquidity_usd / median_price_eth)
        END AS TVL_eth,
        CASE
            WHEN volume_24h IS NULL THEN 0
            ELSE volume_24h
        END AS volume_24h,
        CASE
            WHEN volume_24h_eth IS NULL THEN 0
            ELSE volume_24h_eth
        END AS volume_24h_eth,
        CASE
            WHEN volume_30d IS NULL THEN 0
            ELSE volume_30d
        END AS volume_30d, 
        CASE
            WHEN volume_30d_eth IS NULL THEN 0
            ELSE volume_30d_eth
        END AS volume_30d_eth,  
        CASE
            WHEN volume_all_time IS NULL THEN 0
            ELSE volume_all_time
        END AS volume_all_time, 
        CASE
            WHEN volume_all_time_eth IS NULL THEN 0
            ELSE volume_all_time_eth
        END AS volume_all_time_eth,   
        CASE
            WHEN fees_collected_30d IS NULL THEN 0
            ELSE fees_collected_30d
        END AS fees_collected_30d,        
        CASE
            WHEN fees_collected_30d_eth IS NULL THEN 0
            ELSE fees_collected_30d_eth
        END AS fees_collected_30d_eth,   
        CASE
            WHEN fees_collected IS NULL THEN 0
            ELSE fees_collected
        END AS fees_collected,      
        CASE
            WHEN fees_collected_eth IS NULL THEN 0
            ELSE fees_collected_eth
        END AS fees_collected_eth,   
        CAST("pool_registered" AS DATE) AS pool_registered,
      q.blockchain || 
        CASE 
            WHEN q.blockchain = 'arbitrum' THEN ' 🟦'
            WHEN q.blockchain = 'avalanche_c' THEN ' ⬜ '
            WHEN q.blockchain = 'base' THEN ' 🟨'
            WHEN q.blockchain = 'ethereum' THEN ' Ξ'
            WHEN q.blockchain = 'gnosis' THEN ' 🟩'
            WHEN q.blockchain = 'optimism' THEN ' 🔴'
            WHEN q.blockchain = 'polygon' THEN ' 🟪'
            WHEN q.blockchain = 'zkevm' THEN ' 🟣'
        END 
    AS blockchain
    ,CONCAT('<a target="_blank" href="https://dune.com/balancer/pool-analysis?1.+Pool+ID_t1b222=', SUBSTRING(CAST("poolID" AS VARCHAR), 1, 66), '&4.+Blockchain_t9819b=', q.blockchain, '">View Stats ↗</a>') AS stats,
        CASE
            WHEN q.blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/ethereum/pool/', CAST("poolID" AS VARCHAR), '">balancer ↗</a>')
            WHEN q.blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/arbitrum/pool/', CAST("poolID" AS VARCHAR), '">balancer ↗</a>')
            WHEN q.blockchain = 'polygon' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/polygon/pool/', CAST("poolID" AS VARCHAR), '">balancer ↗</a>')
            WHEN q.blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/gnosis-chain/pool/', CAST("poolID" AS VARCHAR), '">balancer ↗</a>')
            WHEN q.blockchain = 'base' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/base/pool/', CAST("poolID" AS VARCHAR), '">balancer ↗</a>')
            WHEN q.blockchain = 'avalanche_c' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/avalanche/pool/', CAST("poolID" AS VARCHAR), '">balancer ↗</a>')
            WHEN q.blockchain = 'optimism' THEN CONCAT('<a target="_blank" href="https://beets.fi/pool/', CAST("poolID" AS VARCHAR), '">beethoven ↗</a>')
            WHEN q.blockchain = 'zkevm' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/zkevm/pool/', CAST("poolID" AS VARCHAR), '">balancer ↗</a>')
        END AS pool,
        CASE
            WHEN q.blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://etherscan.io/address/0', SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://arbiscan.io/address/0', SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'polygon' THEN CONCAT('<a target "_blank" href="https://polygonscan.com/address/0', SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://gnosisscan.com/address/0', SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'optimism' THEN CONCAT('<a target="_blank" href="https://optimistic.etherscan.io/address/0', SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'avalanche_c' THEN CONCAT('<a target="_blank" href="https://snowtrace.io/address/0', SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'base' THEN CONCAT('<a target="_blank" href="https://basescan.org/address/0', SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'zkevm' THEN CONCAT('<a target "_blank" href="https://zkevm.polygonscan.com/address/0', SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
        END AS scan,
        q.blockchain AS long_chain
    FROM query_2634572 q
    LEFT JOIN volume v ON project_contract_address = SUBSTRING(CAST("poolID" AS VARCHAR), 1, 42) AND q.blockchain = v.blockchain
    LEFT JOIN fees f ON f.pool_address = BYTEARRAY_SUBSTRING("poolID", 1, 20) AND q.blockchain = f.blockchain
    LEFT JOIN (SELECT * 
FROM balancer.liquidity
WHERE day = (SELECT MAX(day) FROM balancer.liquidity WHERE version = '2'))
    l ON l.pool_id =  "poolID" AND q.blockchain = l.blockchain
    LEFT JOIN dune.balancer.result_eth_price p ON l.day = p.day
    WHERE v.blockchain IS NOT NULL
        AND name NOT LIKE '%beets%'
    GROUP BY 1, 2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,20
    ORDER BY 3 DESC
