-- part of a query repo
-- query name: Cross-Chain veBAL boost - TXs
-- query link: https://dune.com/queries/3778295


/* FROM balancer_avalanche_c.OmniVotingEscrowChild_evt_UserBalFromChain */
WITH child_chains_receipts AS(
    SELECT 
    evt_block_time,
    evt_block_number,
    evt_tx_hash,
    evt_index,
    'avalanche_c' AS child_chain,
    User,
    JSON_EXTRACT_SCALAR(userPoint, '$.bias') AS user_point_bias,
    JSON_EXTRACT_SCALAR(userPoint, '$.slope') AS user_point_slope
    FROM balancer_avalanche_c.OmniVotingEscrowChild_evt_UserBalFromChain
    WHERE evt_block_time < TIMESTAMP '2024-05-02 00:00' --snapshot
    
    UNION ALL 
    
    SELECT 
    evt_block_time,
    evt_block_number,
    evt_tx_hash,
    evt_index,
    'arbitrum' AS child_chain,
    User,
    JSON_EXTRACT_SCALAR(userPoint, '$.bias') AS user_point_bias,
    JSON_EXTRACT_SCALAR(userPoint, '$.slope') AS user_point_slope
    FROM balancer_arbitrum.OmniVotingEscrowChild_evt_UserBalFromChain    
    WHERE evt_block_time < TIMESTAMP '2024-05-02 00:00' --snapshot
    
    UNION ALL
    
    SELECT 
    evt_block_time,
    evt_block_number,
    evt_tx_hash,
    evt_index,
    'base' AS child_chain,
    User,
    JSON_EXTRACT_SCALAR(userPoint, '$.bias') AS user_point_bias,
    JSON_EXTRACT_SCALAR(userPoint, '$.slope') AS user_point_slope
    FROM balancer_base.OmniVotingEscrowChild_evt_UserBalFromChain
    WHERE evt_block_time < TIMESTAMP '2024-05-02 00:00' --snapshot
    
    UNION ALL
    
    SELECT 
    evt_block_time,
    evt_block_number,
    evt_tx_hash,
    evt_index,
    'gnosis' AS child_chain,
    User,
    JSON_EXTRACT_SCALAR(userPoint, '$.bias') AS user_point_bias,
    JSON_EXTRACT_SCALAR(userPoint, '$.slope') AS user_point_slope
    FROM balancer_gnosis.OmniVotingEscrowChild_evt_UserBalFromChain  
    WHERE evt_block_time < TIMESTAMP '2024-05-02 00:00' --snapshot
    
    UNION ALL
    
    SELECT 
    evt_block_time,
    evt_block_number,
    evt_tx_hash,
    evt_index,
    'optimism' AS child_chain,
    User,
    JSON_EXTRACT_SCALAR(userPoint, '$.bias') AS user_point_bias,
    JSON_EXTRACT_SCALAR(userPoint, '$.slope') AS user_point_slope
    FROM balancer_optimism.OmniVotingEscrowChild_evt_UserBalFromChain   
    WHERE evt_block_time < TIMESTAMP '2024-05-02 00:00' --snapshot
    
    UNION ALL
    
    SELECT 
    evt_block_time,
    evt_block_number,
    evt_tx_hash,
    evt_index,
    'polygon' AS child_chain,
    User,
    JSON_EXTRACT_SCALAR(userPoint, '$.bias') AS user_point_bias,
    JSON_EXTRACT_SCALAR(userPoint, '$.slope') AS user_point_slope
    FROM balancer_polygon.OmniVotingEscrowChild_evt_UserBalFromChain    
    WHERE evt_block_time < TIMESTAMP '2024-05-02 00:00' --snapshot
    
    UNION ALL
    
    SELECT 
    evt_block_time,
    evt_block_number,
    evt_tx_hash,
    evt_index,
    'zkevm' AS child_chain,
    User,
    JSON_EXTRACT_SCALAR(userPoint, '$.bias') AS user_point_bias,
    JSON_EXTRACT_SCALAR(userPoint, '$.slope') AS user_point_slope
    FROM balancer_zkevm.OmniVotingEscrowChild_evt_UserBalFromChain     
    WHERE evt_block_time < TIMESTAMP '2024-05-02 00:00' --snapshot
    ),
    
vebal_sync AS (
SELECT 
    evt_block_time,
    evt_block_number,
    evt_tx_hash,
    evt_index,
    localUser AS mainnet_address,
    remoteUser AS child_chain_address,
    CASE WHEN lower(chain_name) = 'polygon zkevm'
    THEN 'zkevm'
    WHEN lower(chain_name) = 'avalanche'
    THEN 'avalanche_c'
    ELSE lower(chain_name) 
    END AS dest_chain,
    dstChainId AS child_chain_id,
    JSON_EXTRACT_SCALAR(userPoint, '$.bias') AS user_point_bias,
    JSON_EXTRACT_SCALAR(userPoint, '$.slope') AS user_point_slope
FROM balancer_ethereum.OmniVotingEscrow_evt_UserBalToChain o 
LEFT JOIN layerzero.chain_list l ON o.dstChainId = l.chain_id
WHERE evt_block_time < TIMESTAMP '2024-05-02 00:00') --snapshot

SELECT 
    s.* 
FROM vebal_sync s
INNER JOIN child_chains_receipts r ON s.child_chain_address = r.user
AND s.dest_chain = r.child_chain
AND s.user_point_bias = r.user_point_bias
AND s.user_point_slope = r.user_point_slope
