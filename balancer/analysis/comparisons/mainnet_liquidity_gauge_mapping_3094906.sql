-- part of a query repo
-- query name: mainnet liquidity gauge mapping
-- query link: https://dune.com/queries/3094906


WITH 
    root_gauges AS ( 
        SELECT
            evt_block_time AS rg_evt_block_time
            , evt_block_number AS rg_evt_block_number
            , evt_index AS rg_evt_index
            , gauge AS root_gauge
            , pool
            , NULL AS relativeWeightCap
            , x.contract_address AS rg_factory_address
            , evt_tx_hash AS rg_evt_tx_hash
            , 2 AS rg_version
       FROM balancer_ethereum.LiquidityGaugeFactory_evt_GaugeCreated x
    )
    , rg_kill_unkill AS (
        SELECT 
            x.contract_address AS rg_contract_address
            , x.call_tx_hash AS rg_kill_tx_hash
            , x.call_block_number AS rg_kill_block_number
            , x.call_block_time AS rg_kill_block_time
            , x.call_trace_address AS rg_kill_trace_address
            , y.call_tx_hash AS rg_unkill_tx_hash
            , y.call_block_time AS rg_unkill_block_time
            , y.call_block_number AS rg_unkill_block_number
            , y.call_trace_address AS rg_unkill_trace_address
        FROM balancer_ethereum.LiquidityGaugeV5_call_killGauge x 
        LEFT JOIN 
            (
                SELECT * FROM balancer_ethereum.LiquidityGaugeV5_call_unkillGauge 
                WHERE call_success = True
            ) y
        ON y.contract_address = x.contract_address
        -- Possible to cause issue if kill/unkill happens in same block. See query 3093521
        AND ARRAY[y.call_block_number] >= ARRAY[x.call_block_number]
        INNER JOIN 
            (
                SELECT gauge FROM balancer_ethereum.LiquidityGaugeFactory_evt_GaugeCreated
            ) z
        ON z.gauge = x.contract_address
        WHERE x.call_success = True
    )

    
SELECT root_gauge, pool AS pool_address FROM root_gauges x 
LEFT JOIN rg_kill_unkill y 
ON y.rg_contract_address = x.root_gauge
