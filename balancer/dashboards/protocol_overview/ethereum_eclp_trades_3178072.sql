-- part of a query repo
-- query name: ethereum_eclp_trades
-- query link: https://dune.com/queries/3178072


WITH
    gyro_eclp_pools AS (
        SELECT 
            x.pool
            , y.min_block_time
        FROM gyroscope_ethereum.GyroECLPPoolFactory_evt_PoolCreated x
        LEFT JOIN (
            SELECT min(evt_block_time) AS min_block_time 
            FROM gyroscope_ethereum.GyroECLPPoolFactory_evt_PoolCreated 
        ) y
        ON x.pool IS NOT NULL
    )
SELECT *
FROM balancer_v2_ethereum.trades x
INNER JOIN gyro_eclp_pools y
ON x.block_time >= y.min_block_time
    AND x.project_contract_address = y.pool
