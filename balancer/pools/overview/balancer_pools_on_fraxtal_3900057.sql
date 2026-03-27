-- part of a query repo
-- query name: Balancer Pools on Fraxtal
-- query link: https://dune.com/queries/3900057


   SELECT
        pool_symbol,
        pool_type,
        CAST(protocol_liquidity_usd AS double) AS tvl,
        amount_usd,
        CONCAT('<a target="_blank" href="https://app.balancer.fi/#/fraxtal/pool/', CAST("pool_id" AS VARCHAR), '">app ↗</a>') AS pool,
        CONCAT('<a target="_blank" href="https://fraxscan.io/address/0', SUBSTRING(CAST("pool_id" AS VARCHAR), 2, 41), '">⛓</a>')AS scan,
        pool_address
    FROM dune.balancer.dataset_fraxtal_snapshots q
    WHERE CAST(q.day AS TIMESTAMP) = (SELECT MAX(CAST(day AS TIMESTAMP)) - INTERVAL '1' day FROM dune.balancer.dataset_fraxtal_snapshots)
    ORDER BY 4 DESC
