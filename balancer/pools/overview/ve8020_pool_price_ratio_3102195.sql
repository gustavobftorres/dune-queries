-- part of a query repo
-- query name: ve8020 Pool Price Ratio
-- query link: https://dune.com/queries/3102195


WITH 
  pool_info AS(
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
      
      governance_token AS (
        SELECT
          dp.day,
          tw.token,
          COALESCE(up.price,dp.price) as price
        FROM tokens_and_weight AS tw
        LEFT JOIN dex_prices AS dp ON tw.token = dp.token
        LEFT JOIN usd_prices AS up ON tw.token = up.token AND dp.day = up.day
        WHERE
          dp.day >= (SELECT day FROM pool_info) + INTERVAL '1' day AND tw.weight = .8
         ),
         
         base_token AS (
        SELECT
          dp.day,
          tw.token,
          COALESCE(up.price,dp.price) as price
        FROM tokens_and_weight AS tw
        LEFT JOIN dex_prices AS dp ON tw.token = dp.token
        LEFT JOIN usd_prices AS up ON tw.token = up.token AND dp.day = up.day
        WHERE
          dp.day >= (SELECT day FROM pool_info) + INTERVAL '1' day AND tw.weight = .2
         )
    
    SELECT r.day, r.price, w.price as w_price, r.price/w.price as ratio
    FROM governance_token r 
    LEFT JOIN base_token w ON r.day = w.day