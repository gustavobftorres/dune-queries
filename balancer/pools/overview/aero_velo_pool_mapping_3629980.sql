-- part of a query repo
-- query name: AERO/VELO Pool Mapping
-- query link: https://dune.com/queries/3629980


SELECT 
    'aerodrome' AS project,
    'base' AS blockchain,
    pool, 
    CASE WHEN stable THEN 'stable'
    ELSE 'volatile'
    END AS pool_type
FROM aerodrome_base.PoolFactory_evt_PoolCreated

UNION ALL

SELECT 
    'velodrome' AS project,
    'optimism' AS blockchain,
    pool, 
    CASE WHEN stable THEN 'stable'
    ELSE 'volatile'
    END AS pool_type
FROM velodrome_v2_optimism.PoolFactory_evt_PoolCreated

UNION ALL

SELECT 
    'velodrome' AS project,
    'optimism' AS blockchain,
    pool, 
    'concentrated stable' AS pool_type
FROM velodrome_v2_optimism.CLFactory_evt_PoolCreated