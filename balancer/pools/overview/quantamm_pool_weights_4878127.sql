-- part of a query repo
-- query name: quantAMM pool weights
-- query link: https://dune.com/queries/4878127


WITH token_data AS (
        SELECT
            pool,
            ARRAY_AGG(FROM_HEX(json_extract_scalar(token, '$.token')) ORDER BY token_index) AS tokens 
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM balancer_testnet_sepolia.Vault_evt_PoolRegistered
        ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
        GROUP BY 1
    ),

      quantamm AS(SELECT
          c.pool AS pool_id,
          evt_block_time,
          evt_block_number,
          evt_index,
          t.token AS token_address,
          t.pos,
          w.weight AS normalized_weight,
          FROM_UNIXTIME(lastUpdateTime) AS last_update,
          ROW_NUMBER() OVER (PARTITION BY c.pool, t.token ORDER BY evt_block_number DESC, evt_index DESC) AS rn
        FROM token_data c
        INNER JOIN balancer_testnet_sepolia.quantammweightedpool_evt_weightsupdated cc
        ON c.pool = cc.contract_address
        CROSS JOIN UNNEST(c.tokens) WITH ORDINALITY t(token, pos)
        CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weight, pos)
        WHERE t.pos = w.pos
        ORDER BY 1 DESC, 5 DESC, 8 DESC)

        SELECT
            pool_id,
            pos,
            token_address,
            normalized_weight
        FROM quantamm
        WHERE rn = 1
 