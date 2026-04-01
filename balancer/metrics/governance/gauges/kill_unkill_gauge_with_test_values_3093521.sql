-- part of a query repo
-- query name: kill/unkill gauge with test values
-- query link: https://dune.com/queries/3093521


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
    SELECT * FROM --balancer_ethereum.PolygonZkEVMRootGauge_call_unkillGauge 
    -- Input test values here that can later be used for properly specificing edge case handeling.
    -- See path tree below.
        (
            SELECT * FROM (
                VALUES
                    (0x227534d3f9ae72cb5b31e1d0a27be1a2859c6cc8, True, 0x4cac17671a7490b4c9816323be399493b8ff1757fc26f3d9d47a0efdce83d64f, ARRAY[0, 0, 127, 3, 2, 1], timestamp '2023-08-22 15:06', 17749325)
            )
            AS v (
                contract_address
                , call_success
                , call_tx_hash
                , call_trace_address
                , call_block_time
                , call_block_number
            )
        )
    WHERE call_success = True
) y
    ON y.contract_address = x.contract_address
/* Possible to cause issue if kill/unkill happens in same block. 

   Kill/unkill same block
   ├── IF yes
   │   └── kill/unkill same tx?
   │       ├── IF yes
   │       │   └── unkill trace_address > kill trace_address
   │       └── IF no
   │           └── unkill tx_index > kill tx_index == True?
   └── IF no
       └── unkill block > kill block == True? 
       
*/
AND ARRAY[y.call_block_number] >= ARRAY[x.call_block_number]
INNER JOIN (SELECT gauge FROM balancer_ethereum.PolygonZkEVMRootGaugeFactory_evt_GaugeCreated) z
    ON z.gauge = x.contract_address
WHERE x.call_success = True