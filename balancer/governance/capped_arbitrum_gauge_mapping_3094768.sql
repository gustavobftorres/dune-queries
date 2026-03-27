-- part of a query repo
-- query name: capped arbitrum gauge mapping
-- query link: https://dune.com/queries/3094768


WITH 
    root_gauges AS ( 
        SELECT
            evt_block_time AS rg_evt_block_time
            , evt_block_number AS rg_evt_block_number
            , evt_index AS rg_evt_index
            , gauge AS root_gauge
            , recipient
            , relativeWeightCap
            , x.contract_address AS rg_factory_address
            , evt_tx_hash AS rg_evt_tx_hash
            , 2 AS rg_version
        FROM 
            (
                SELECT * FROM balancer_ethereum.CappedArbitrumRootGaugeFactory_evt_GaugeCreated
                CROSS JOIN (SELECT count(*) AS evt_record_count FROM balancer_ethereum.CappedArbitrumRootGaugeFactory_evt_GaugeCreated)
            ) x
        INNER JOIN 
            (
                SELECT * FROM balancer_ethereum.CappedArbitrumRootGaugeFactory_call_create
                CROSS JOIN (SELECT count(*) AS call_record_count FROM balancer_ethereum.CappedArbitrumRootGaugeFactory_call_create WHERE call_success = True) 
                WHERE call_success = True
            ) y
        ON y.output_0 = x.gauge
        AND y.call_record_count = x.evt_record_count -- Test .. No results return if False
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
        FROM balancer_ethereum.CappedArbitrumRootGauge_call_killGauge x 
        LEFT JOIN 
            (
                SELECT * FROM balancer_ethereum.CappedArbitrumRootGauge_call_unkillGauge 
                WHERE call_success = True
            ) y
        ON y.contract_address = x.contract_address
        -- Possible to cause issue if kill/unkill happens in same block. See query 3093521
        AND ARRAY[y.call_block_number] >= ARRAY[x.call_block_number]
        INNER JOIN 
            (
                SELECT gauge FROM balancer_ethereum.CappedArbitrumRootGaugeFactory_evt_GaugeCreated
            ) z
        ON z.gauge = x.contract_address
        WHERE x.call_success = True
    )
    , child_chain_gauges AS (
        SELECT
            x.evt_block_time AS ccgc_evt_block_time
            , x.evt_block_number AS ccgc_evt_block_number
            , x.evt_index AS ccgc_evt_index
            , x.gauge AS child_chain_gauge
            -- Pool Address identified through call data
            , y.pool AS pool_address
            , x.contract_address AS ccg_factory_address
            , x.evt_tx_hash AS ccgc_evt_tx_hash
            , 2 AS ccg_version
            -- Pool creation details
            , z.evt_tx_hash AS pc_evt_tx_hash
            , z.evt_index AS pc_evt_index
            , z.evt_block_time AS pc_evt_block_time
            , z.evt_block_number AS pc_evt_block_number
            , z.poolId AS pool_id
            , z.specialization AS pool_specialization
        FROM 
            (
                SELECT * FROM balancer_arbitrum.ChildChainGaugeFactory_evt_GaugeCreated
                CROSS JOIN (SELECT count(*) AS evt_record_count FROM balancer_arbitrum.ChildChainGaugeFactory_evt_GaugeCreated)
            ) x
        INNER JOIN 
            (
                SELECT * FROM balancer_arbitrum.ChildChainGaugeFactory_call_create
                CROSS JOIN (SELECT count(*) AS call_record_count FROM balancer_arbitrum.ChildChainGaugeFactory_call_create WHERE call_success = True) 
                WHERE call_success = True
            ) y
        ON y.output_0 = x.gauge
        AND y.call_record_count = x.evt_record_count -- Test .. No results return if False
        -- Add pool creation (pc) details
        LEFT JOIN 
            (
                SELECT * FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered
            ) z
        ON z.poolAddress = y.pool
    )
    , ccg_kill_unkill AS (
        SELECT 
            x.contract_address AS ccg_contract_address
            , x.call_tx_hash AS ccg_kill_tx_hash
            , x.call_block_number AS ccg_kill_block_number
            , x.call_block_time AS ccg_kill_block_time
            , x.call_trace_address AS ccg_kill_trace_address
            , y.call_tx_hash AS ccg_unkill_tx_hash
            , y.call_block_time AS ccg_unkill_block_time
            , y.call_block_number AS ccg_unkill_block_number
            , y.call_trace_address AS ccg_unkill_trace_address
        FROM balancer_arbitrum.ChildChainGauge_call_killGauge x 
        LEFT JOIN 
            (
                SELECT * FROM balancer_arbitrum.ChildChainGauge_call_unkillGauge 
                WHERE call_success = True
            ) y
        ON y.contract_address = x.contract_address
        -- Possible to cause issue if kill/unkill happens in same block. See query 3093521
        AND ARRAY[y.call_block_number] >= ARRAY[x.call_block_number]
        INNER JOIN 
            (
                SELECT gauge FROM balancer_arbitrum.ChildChainGaugeFactory_evt_GaugeCreated
            ) z 
        ON z.gauge = x.contract_address
        WHERE x.call_success = True
    )
    
    
SELECT root_gauge, recipient, child_chain_gauge, pool_address FROM root_gauges x 
LEFT JOIN rg_kill_unkill y 
ON y.rg_contract_address = x.root_gauge
LEFT JOIN 
    (
        SELECT * FROM child_chain_gauges x 
        LEFT JOIN ccg_kill_unkill y 
        ON y.ccg_contract_address = x.child_chain_gauge
    ) z
ON z.child_chain_gauge = x.recipient