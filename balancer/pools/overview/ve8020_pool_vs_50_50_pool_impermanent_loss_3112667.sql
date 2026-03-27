-- part of a query repo
-- query name: ve8020 Pool vs. 50/50 Pool Impermanent Loss
-- query link: https://dune.com/queries/3112667


WITH ve8020_il AS(
   WITH pool_info AS(
        SELECT DATE_TRUNC('day', evt_block_time) as day
        FROM balancer_v2_{{Blockchain}}.Vault_evt_PoolRegistered
        WHERE poolAddress = {{Pool Address}}  
    ),
    
    usd_prices AS (
        SELECT
          DATE_TRUNC('day', minute) AS day,
          contract_address AS token,
          AVG(price) AS price
        FROM prices.usd
        WHERE
          blockchain = '{{Blockchain}}'
          AND DATE_TRUNC('day', minute) >= (SELECT day FROM pool_info) + INTERVAL '1' day
        GROUP BY 1, 2),
    
    dex_prices AS (
        SELECT
          DATE_TRUNC('day', hour) AS day,
          contract_address AS token,
          AVG(median_price) AS price
        FROM dex.prices
        WHERE
          blockchain = '{{Blockchain}}'
          AND DATE_TRUNC('day', hour) >= (SELECT day FROM pool_info) + INTERVAL '1' day
        GROUP BY 1, 2),
        
      tokens_and_weight AS (
        SELECT
          token_address AS token,
          normalized_weight AS weight
        FROM balancer_v2_{{Blockchain}}.pools_tokens_weights
        WHERE BYTEARRAY_SUBSTRING(pool_id,1,20) = {{Pool Address}}),
      
      beginning_token_price AS (
        SELECT
          tw.token,
          tw.weight,
          COALESCE(up.price,dp.price) as price
        FROM tokens_and_weight AS tw
        LEFT JOIN dex_prices AS dp ON tw.token = dp.token
        LEFT JOIN usd_prices AS up ON tw.token = up.token AND dp.day = up.day
        WHERE
          dp.day = (SELECT day FROM pool_info) + INTERVAL '1' day
         ),
          
      delta AS (
        SELECT
          dp.day,
          dp.token,
          COALESCE(up.price,dp.price) / CAST(btp.price AS DOUBLE) AS delta,
          btp.weight
        FROM tokens_and_weight AS tw
        LEFT JOIN dex_prices AS dp ON tw.token = dp.token
        LEFT JOIN usd_prices AS up ON tw.token = up.token AND dp.day = up.day
        LEFT JOIN beginning_token_price AS btp ON tw.token = btp.token
      ),
      
      average_delta AS (
        SELECT
          day,
          token,
          weight,
          AVG(delta) AS delta
        FROM delta
        WHERE token <> {{Pool Address}}
        GROUP BY 1, 2, 3),
        
      delta_functions AS (
        SELECT
          day,
          SUM(delta * weight) AS hold_value_usd,
          EXP(SUM(LN(POWER(delta, weight)))) AS pool_value_usd
        FROM average_delta
        GROUP BY 1),
        
      impermanent_loss_by_day AS (
        SELECT
          day,
          (pool_value_usd / CAST(hold_value_usd AS DOUBLE)) - 1 AS impermanent_loss
        FROM delta_functions
      )
        
    SELECT
      day,
      've8020' AS name,
      impermanent_loss AS impermanent_loss
    FROM impermanent_loss_by_day
        WHERE day > (SELECT day FROM pool_info) + INTERVAL '1' day),   
    
fifty_il AS(
    WITH pool_info AS(
        SELECT DATE_TRUNC('day', evt_block_time) as day
        FROM balancer_v2_{{Blockchain}}.Vault_evt_PoolRegistered
        WHERE poolAddress = {{Pool Address}}  
    ),
    
    usd_prices AS (
        SELECT
          DATE_TRUNC('day', minute) AS day,
          contract_address AS token,
          AVG(price) AS price
        FROM prices.usd
        WHERE
          blockchain = '{{Blockchain}}'
          AND DATE_TRUNC('day', minute) >= (SELECT day FROM pool_info) + INTERVAL '1' day
        GROUP BY 1, 2),
    
    dex_prices AS (
        SELECT
          DATE_TRUNC('day', hour) AS day,
          contract_address AS token,
          AVG(median_price) AS price
        FROM dex.prices
        WHERE
          blockchain = '{{Blockchain}}'
          AND DATE_TRUNC('day', hour) >= (SELECT day FROM pool_info) + INTERVAL '1' day
        GROUP BY 1, 2),
        
      tokens_and_weight AS (
        SELECT
          token_address AS token,
          .5 AS weight
        FROM balancer_v2_{{Blockchain}}.pools_tokens_weights
        WHERE BYTEARRAY_SUBSTRING(pool_id,1,20) = {{Pool Address}}),
      
      beginning_token_price AS (
        SELECT
          tw.token,
          tw.weight,
          COALESCE(up.price,dp.price) as price
        FROM tokens_and_weight AS tw
        LEFT JOIN dex_prices AS dp ON tw.token = dp.token
        LEFT JOIN usd_prices AS up ON tw.token = up.token AND dp.day = up.day
        WHERE
          dp.day = (SELECT day FROM pool_info) + INTERVAL '1' day
         ),
          
      delta AS (
        SELECT
          dp.day,
          dp.token,
          COALESCE(up.price,dp.price) / CAST(btp.price AS DOUBLE) AS delta,
          btp.weight
        FROM tokens_and_weight AS tw
        LEFT JOIN dex_prices AS dp ON tw.token = dp.token
        LEFT JOIN usd_prices AS up ON tw.token = up.token AND dp.day = up.day
        LEFT JOIN beginning_token_price AS btp ON tw.token = btp.token
      ),
      
      average_delta AS (
        SELECT
          day,
          token,
          weight,
          AVG(delta) AS delta
        FROM delta
        WHERE token <> {{Pool Address}}
        GROUP BY 1, 2, 3),
        
      delta_functions AS (
        SELECT
          day,
          SUM(delta * weight) AS hold_value_usd,
          EXP(SUM(LN(POWER(delta, weight)))) AS pool_value_usd
        FROM average_delta
        GROUP BY 1),
        
      impermanent_loss_by_day AS (
        SELECT
          day,
          (pool_value_usd / CAST(hold_value_usd AS DOUBLE)) - 1 AS impermanent_loss
        FROM delta_functions
      )
        
    SELECT
      day,
      '5050' AS name,
      impermanent_loss AS impermanent_loss
    FROM impermanent_loss_by_day
        WHERE day > (SELECT day FROM pool_info) + INTERVAL '1' day)
    
SELECT v.day, v.impermanent_loss as ve8020, f.impermanent_loss as "5050"
FROM ve8020_il v
LEFT JOIN fifty_il f ON f.day = v.day
WHERE v.impermanent_loss < 1 AND f.impermanent_loss < 1