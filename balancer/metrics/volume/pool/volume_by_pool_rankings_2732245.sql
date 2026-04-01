-- part of a query repo
-- query name: Volume by Pool Rankings 🏆
-- query link: https://dune.com/queries/2732245


WITH
    all_ranked AS (
        SELECT
            r1.seven_day_vol_rank,
            r1.seven_day_volume,
            r1.blockchain,
            r1.pool,
            r2.one_day_vol_rank,
            r2.one_day_volume
        FROM query_6773488 r1
        LEFT JOIN query_6773496 r2 ON r1.pool = r2.pool AND r1.blockchain = r2.blockchain
        WHERE r1.seven_day_volume IS NOT NULL
          AND r2.one_day_volume IS NOT NULL
          AND r1.seven_day_vol_rank <= 100
    ),
    custom_labels AS (
        SELECT address, blockchain, kind, name
        FROM query_2846430
    )
SELECT
    seven_day_vol_rank,
    seven_day_volume,
    a.blockchain ||
        CASE
            WHEN a.blockchain = 'arbitrum'    THEN ' 🟦 |'
            WHEN a.blockchain = 'avalanche_c' THEN ' ⬜  |'
            WHEN a.blockchain = 'base'        THEN ' 🟨 |'
            WHEN a.blockchain = 'ethereum'    THEN ' Ξ |'
            WHEN a.blockchain = 'gnosis'      THEN ' 🟩 |'
            WHEN a.blockchain = 'optimism'    THEN ' 🔴 |'
            WHEN a.blockchain = 'polygon'     THEN ' 🟪 |'
            WHEN a.blockchain = 'zkevm'       THEN ' 🟣 |'
        END
    AS blockchain,
    COALESCE(
        cl.name,
        l.name,
        substring(CAST(a.pool AS VARCHAR), 1, 6) || '...' || substring(CAST(a.pool AS VARCHAR), 39, 42)
    ) AS sym,
    a.pool,
    one_day_vol_rank,
    one_day_volume,
    cl.address AS custom_address,
    cl.name    AS custom_label_name,
    l.address  AS labels_address,
    l.name     AS labels_name
FROM all_ranked a
LEFT JOIN labels.balancer_v2_pools l
    ON a.blockchain = l.blockchain AND a.pool = l.address
LEFT JOIN custom_labels cl
    ON a.blockchain = cl.blockchain AND a.pool = cl.address
ORDER BY seven_day_vol_rank ASC