-- part of a query repo
-- query name: polygon zkevm gauge mapping (need chain support to finish)
-- query link: https://dune.com/queries/3091308


SELECT
    evt_block_time
    , evt_block_number
    , evt_index
    , gauge AS root_gauge
    , recipient
    , relativeWeightCap
    , x.contract_address AS rg_factory_address
    , evt_tx_hash
    , 2 AS version
    , kill_tx_hash
    , kill_block_number
    , kill_block_time
    , kill_trace_address
    , unkill_tx_hash
    , unkill_block_number
    , unkill_block_time
    , unkill_trace_address
FROM 
    (
        SELECT * FROM balancer_ethereum.PolygonZkEVMRootGaugeFactory_evt_GaugeCreated
        CROSS JOIN (SELECT count(*) AS evt_record_count FROM balancer_ethereum.PolygonZkEVMRootGaugeFactory_evt_GaugeCreated)
    ) x
INNER JOIN 
    (
        SELECT * FROM balancer_ethereum.PolygonZkEVMRootGaugeFactory_call_create
        CROSS JOIN (SELECT count(*) AS call_record_count FROM balancer_ethereum.PolygonZkEVMRootGaugeFactory_call_create WHERE call_success = True) 
        WHERE call_success = True
    ) y
ON y.output_0 = x.gauge
AND y.call_record_count = x.evt_record_count -- Test .. No results return if False
LEFT JOIN 
    (
        SELECT 
            x.contract_address
            , x.call_tx_hash AS kill_tx_hash
            , x.call_block_number AS kill_block_number
            , x.call_block_time AS kill_block_time
            , x.call_trace_address AS kill_trace_address
            , y.call_tx_hash AS unkill_tx_hash
            , y.call_block_time AS unkill_block_time
            , y.call_block_number AS unkill_block_number
            , y.call_trace_address AS unkill_trace_address
        FROM balancer_ethereum.PolygonZkEVMRootGauge_call_killGauge x 
        LEFT JOIN (
            SELECT * FROM balancer_ethereum.PolygonZkEVMRootGauge_call_unkillGauge 
            WHERE call_success = True
        ) y
            ON y.contract_address = x.contract_address
        -- Possible to cause issue if kill/unkill happens in same block. See query 3093521
        AND ARRAY[y.call_block_number] >= ARRAY[x.call_block_number]
        INNER JOIN (SELECT gauge FROM balancer_ethereum.PolygonZkEVMRootGaugeFactory_evt_GaugeCreated) z
            ON z.gauge = x.contract_address
        WHERE x.call_success = True
    ) z
ON z.contract_address = x.gauge