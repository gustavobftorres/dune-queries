-- part of a query repo
-- query name: Governance/Base Tokens Volatility Risk
-- query link: https://dune.com/queries/3118700


WITH 

 pool_info AS(
        SELECT DATE_TRUNC('day', evt_block_time) as day
        FROM balancer_v2_{{Blockchain}}.Vault_evt_PoolRegistered
        WHERE poolAddress = {{Pool Address}}  
    ),
    
    usd_prices AS (
        SELECT
          DATE_TRUNC('hour', minute) AS hour,
          contract_address AS token,
          AVG(price) AS price
        FROM prices.usd
        WHERE
          blockchain = '{{Blockchain}}'
          AND DATE_TRUNC('day', minute) >= (SELECT day FROM pool_info) + INTERVAL '1' day
        GROUP BY 1, 2),
    
    dex_prices AS (
        SELECT
          hour,
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

    governance_token_price AS (
        SELECT
          dp.hour as time,
          AVG(COALESCE(up.price,dp.price)) as avg_price
        FROM tokens_and_weight AS tw
        LEFT JOIN dex_prices AS dp ON tw.token = dp.token
        LEFT JOIN usd_prices AS up ON tw.token = up.token AND dp.hour = up.hour
        WHERE
          DATE_TRUNC('day',dp.hour) >= (SELECT day FROM pool_info) + INTERVAL '1' day AND tw.weight = .8
        GROUP BY 1
        ORDER BY 1 DESC 
    ),
    
    governance_token_log_data AS (
        SELECT time, log_returns
        FROM (
        SELECT time, ln(avg_price/lag(avg_price) over (ORDER BY time)) AS log_returns
        FROM governance_token_price 
        ORDER BY time DESC
        ) x 
        WHERE log_returns > 0 -- ignore negative values 
    ),
    
    governance_token_volatility AS (
        SELECT date_trunc('day', time) as time, 
        sqrt(variance(log_returns)) AS volatility_risk_governance_token
        FROM governance_token_log_data
        GROUP BY 1
    ),
    
    base_token_price AS (
            SELECT
              dp.hour as time,
              AVG(COALESCE(up.price,dp.price)) as avg_price
            FROM tokens_and_weight AS tw
            LEFT JOIN dex_prices AS dp ON tw.token = dp.token
            LEFT JOIN usd_prices AS up ON tw.token = up.token AND dp.hour = up.hour
            WHERE
              DATE_TRUNC('day',dp.hour) >= (SELECT day FROM pool_info) + INTERVAL '1' day AND tw.weight = .2
        GROUP BY 1
        ORDER BY 1 DESC 
    ),
    
    base_token_log_data AS (
        SELECT time, log_returns
        FROM (
        SELECT time, ln(avg_price/lag(avg_price) over (ORDER BY time)) AS log_returns
        FROM base_token_price 
        ORDER BY time DESC
        ) x 
        WHERE log_returns > 0 -- ignore negative values 
    ),
    
    base_token_volatility AS (
        SELECT DATE_TRUNC('day', time) AS time, sqrt(variance(log_returns)) AS volatility_risk_base_token
        FROM base_token_log_data
        GROUP BY 1
    ),
    
    summary AS (
    SELECT *
    FROM governance_token_volatility
    left join base_token_volatility using(time)
    )
    
    SELECT *
    FROM summary
    ORDER BY time DESC