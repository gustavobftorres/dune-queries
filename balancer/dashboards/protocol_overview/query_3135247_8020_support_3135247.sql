-- part of a query repo
-- query name: (query_3135247) 8020_support
-- query link: https://dune.com/queries/3135247


WITH 
pool_registered as(
        SELECT DATE_TRUNC('day', evt_block_time) as day, poolAddress, 'arbitrum' as blockchain
        FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered
        UNION ALL
        SELECT DATE_TRUNC('day', evt_block_time) as day, poolAddress, 'ethereum' as blockchain
        FROM balancer_v2_ethereum.Vault_evt_PoolRegistered),

tokens_and_weights AS (
        SELECT
          BYTEARRAY_SUBSTRING(pool_id,1,20) as pool_address,
          'arbitrum' as blockchain,
          token_address AS token,
          normalized_weight AS weight
        FROM balancer_v2_arbitrum.pools_tokens_weights
        UNION ALL 
        SELECT
          BYTEARRAY_SUBSTRING(pool_id,1,20) as pool_address,
          'ethereum' as blockchain,
          token_address AS token,
          normalized_weight AS weight
        FROM balancer_v2_ethereum.pools_tokens_weights)
        
SELECT r.day, w.* FROM pool_registered r
LEFT JOIN tokens_and_weights w ON r.poolAddress = w.pool_address AND r.blockchain = w.blockchain