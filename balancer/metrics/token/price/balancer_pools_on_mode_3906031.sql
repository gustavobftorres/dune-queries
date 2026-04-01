-- part of a query repo
-- query name: Balancer Pools on Mode
-- query link: https://dune.com/queries/3906031


   SELECT
        pool_symbol,
        pool_type,
        protocol_liquidity_usd AS tvl,
        CASE
            WHEN amount_usd IS NULL THEN 0
            ELSE amount_usd
        END AS amount_usd,
        CONCAT('<a target="_blank" href="https://app.balancer.fi/#/mode/pool/', CAST("pool_id" AS VARCHAR), '">app ↗</a>') AS pool,
        CONCAT('<a target="_blank" href="https://fraxscan.io/address/0', SUBSTRING(CAST("pool_id" AS VARCHAR), 2, 41), '">⛓</a>')AS scan,
        pool_address
    FROM dune.balancer.dataset_mode_snapshots q
    WHERE CAST(q.day AS TIMESTAMP) = (SELECT MAX(CAST(day AS TIMESTAMP)) - INTERVAL '1' day FROM dune.balancer.dataset_mode_snapshots)
    ORDER BY 3 DESC
