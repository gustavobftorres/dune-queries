-- part of a query repo
-- query name: Pool balance snapshots
-- query link: https://dune.com/queries/4786655


SELECT
    from_unixtime(day) AS day,
    'arbitrum' AS blockchain,
    SUBSTRING(pool, 1, 42) AS pool_id,
    TRY_CAST(token_balance AS DOUBLE) AS token_balance,
    ROW_NUMBER() OVER (PARTITION BY day, pool)  AS token_index
FROM dune.balancer.dataset_v3_arbitrum_balance_snapshots

UNION

SELECT
    from_unixtime(day) AS day,
    'base' AS blockchain,
    SUBSTRING(pool, 1, 42) AS pool_id,
    TRY_CAST(token_balance AS DOUBLE) AS token_balance,
    ROW_NUMBER() OVER (PARTITION BY day, pool)  AS token_index
FROM dune.balancer.dataset_v3_base_balance_snapshots

UNION

SELECT
    from_unixtime(day) AS day,
    'gnosis' AS blockchain,
    SUBSTRING(pool, 1, 42) AS pool_id,
    TRY_CAST(token_balance AS DOUBLE) AS token_balance,
    ROW_NUMBER() OVER (PARTITION BY day, pool)  AS token_index
FROM dune.balancer.dataset_v3_gnosis_balance_snapshots

UNION

SELECT
    from_unixtime(day) AS day,
    'ethereum' AS blockchain,
    SUBSTRING(pool, 1, 42) AS pool_id,
    TRY_CAST(token_balance AS DOUBLE) AS token_balance,
    ROW_NUMBER() OVER (PARTITION BY day, pool)  AS token_index
FROM dune.balancer.dataset_v3_ethereum_balance_snapshots

