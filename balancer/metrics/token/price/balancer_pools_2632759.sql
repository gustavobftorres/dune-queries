-- part of a query repo
-- query name: Balancer Pools
-- query link: https://dune.com/queries/2632759


WITH volume AS (
    SELECT
        CAST(project_contract_address AS VARCHAR) AS project_contract_address,
        blockchain,
        SUM(amount_usd) AS amount_usd
    FROM balancer.trades
    WHERE
        block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '1' DAY
    GROUP BY 1, 2
)

    SELECT
        UPPER(SUBSTRING("name", 1, 60)) AS name,
        "poolID",
        CASE
            WHEN "TVL" IS NULL THEN 0
            ELSE "TVL"
        END AS "TVL",
        CASE
            WHEN amount_usd IS NULL THEN 0
            ELSE amount_usd
        END AS amount_usd,
        "pool_registered",
      q.blockchain || 
        CASE 
            WHEN q.blockchain = 'arbitrum' THEN ' 🟦 '
            WHEN q.blockchain = 'avalanche_c' THEN ' ⬜  '
            WHEN q.blockchain = 'base' THEN ' 🟨 '
            WHEN q.blockchain = 'ethereum' THEN ' Ξ '
            WHEN q.blockchain = 'gnosis' THEN ' 🟩 '
            WHEN q.blockchain = 'optimism' THEN ' 🔴 '
            WHEN q.blockchain = 'polygon' THEN ' 🟪 '
            WHEN q.blockchain = 'zkevm' THEN ' 🟣 '
        END 
    AS blockchain,
        CONCAT('<a href="https://dune.com/balancer/pool-analysis?1.+Pool+ID_t1b222=', SUBSTRING(CAST("poolID" AS VARCHAR), 1, 66), '&4.+q.Blockchain_t9819b=', q.blockchain, '">View Stats ↗</a>') AS stats,
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
            WHEN q.blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://etherscan.io/address/0', BYTEARRAY_SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://arbiscan.io/address/0', BYTEARRAY_SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'polygon' THEN CONCAT('<a target "_blank" href="https://polygonscan.com/address/0', BYTEARRAY_SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://gnosisscan.com/address/0', BYTEARRAY_SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'optimism' THEN CONCAT('<a target="_blank" href="https://optimistic.etherscan.io/address/0', BYTEARRAY_SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'avalanche_c' THEN CONCAT('<a target="_blank" href="https://snowtrace.io/address/0', BYTEARRAY_SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'base' THEN CONCAT('<a target="_blank" href="https://basescan.org/address/0', BYTEARRAY_SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'polygon' THEN CONCAT('<a target "_blank" href="https://zkevm.polygonscan.com/address/0', BYTEARRAY_SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
        END AS scan
    FROM query_2634572 q
    LEFT JOIN volume v ON project_contract_address = SUBSTRING(CAST("poolID" AS VARCHAR), 1, 42) AND q.blockchain = v.blockchain
    WHERE "TVL" IS NOT NULL
    ORDER BY 4 DESC
