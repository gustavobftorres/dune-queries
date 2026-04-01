-- part of a query repo
-- query name: Balancer CoWSwap AMM Pools
-- query link: https://dune.com/queries/3965064


WITH volume AS (
    SELECT
        CAST(project_contract_address AS VARCHAR) AS project_contract_address,
        blockchain,
        SUM(amount_usd) AS amount_usd
    FROM balancer_cowswap_amm.trades
    WHERE
        block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '1' DAY
    GROUP BY 1, 2
)

    SELECT
        CASE 
            WHEN q.blockchain = 'arbitrum' THEN '🟦 '
            WHEN q.blockchain = 'ethereum' THEN 'Ξ '
            WHEN q.blockchain = 'gnosis' THEN '🟩 '
        END 
        || UPPER(SUBSTRING("name", 1, 60)) AS name,
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
            WHEN q.blockchain = 'arbitrum' THEN ' 🟦'
            WHEN q.blockchain = 'ethereum' THEN ' Ξ'
            WHEN q.blockchain = 'gnosis' THEN ' 🟩'
        END 
    AS blockchain 
    ,CONCAT('<a target="_blank" href="https://dune.com/balancer/balancer-cowswap-amm-pool?1.+Pool+Address_tc1e0a=', SUBSTRING(CAST("poolID" AS VARCHAR), 1, 66), '&4.+Blockchain_t4bd30=', q.blockchain, '">Stats ↗</a>') AS stats,
        CASE
            WHEN q.blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://balancer.fi/pools/ethereum/cow/', CAST("poolID" AS VARCHAR), '">app ↗</a>')
            WHEN q.blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://balancer.fi/pools/arbitrum/cow/', CAST("poolID" AS VARCHAR), '">app ↗</a>')
            WHEN q.blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://balancer.fi/pools/gnosis/cow/', CAST("poolID" AS VARCHAR), '">app ↗</a>')
        END AS pool,
        CASE
            WHEN q.blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://etherscan.io/address/0', SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://arbiscan.io/address/0', SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN q.blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://gnosisscan.com/address/0', SUBSTRING(CAST("poolID" AS VARCHAR), 2, 41), '">⛓</a>')        
            END AS scan
    FROM query_3954616 q
    LEFT JOIN volume v ON project_contract_address = SUBSTRING(CAST("poolID" AS VARCHAR), 1, 42) AND q.blockchain = v.blockchain
    WHERE
         "TVL" IS NOT NULL
    ORDER BY 3 DESC
