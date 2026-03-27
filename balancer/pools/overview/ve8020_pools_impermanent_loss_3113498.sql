-- part of a query repo
-- query name: ve8020 Pools Impermanent Loss
-- query link: https://dune.com/queries/3113498


WITH
  prices AS (
    SELECT
      DATE_TRUNC('day', hour) AS day,
      blockchain,
      contract_address AS token,
      AVG(median_price) AS price
    FROM dex.prices
    WHERE
      blockchain IN ('arbitrum', 'ethereum', 'polygon')
      AND DATE_TRUNC('day', hour) >= CURRENT_TIMESTAMP - INTERVAL '1' month
    GROUP BY 1, 2, 3),
    
  tokens_and_weight AS (
    SELECT
      BYTEARRAY_SUBSTRING(pool_id,1,20) as pool_id,
      'arbitrum' as blockchain,
      token_address AS token,
      normalized_weight AS weight
    FROM balancer_v2_arbitrum.pools_tokens_weights
    WHERE BYTEARRAY_SUBSTRING(pool_id,1,20) IN 
        (0x32df62dc3aed2cd6224193052ce665dc18165841,
        0x569061e2d807881f4a33e1cbe1063bc614cb75a4,
        0x85ec6ae01624ae0d2a04d0ffaad3a25884c7d0f3,
        0x3efd3e18504dc213188ed2b694f886a305a6e5ed)
    
    UNION ALL
    
    SELECT
      BYTEARRAY_SUBSTRING(pool_id,1,20) as pool_id,
      'polygon' as blockchain,
      token_address AS token,
      normalized_weight AS weight
    FROM balancer_v2_polygon.pools_tokens_weights
    WHERE BYTEARRAY_SUBSTRING(pool_id,1,20) IN 
        (0xe2f706ef1f7240b803aae877c9c762644bb808d8,
        0xae8f935830f6b418804836eacb0243447b6d977c,
        0xb204bf10bc3a5435017d3db247f56da601dfe08a)
    
    UNION ALL
     
    SELECT
      BYTEARRAY_SUBSTRING(pool_id,1,20) as pool_id,
      'optimism' as blockchain,
      token_address AS token,
      normalized_weight AS weight
    FROM balancer_v2_optimism.pools_tokens_weights
    WHERE BYTEARRAY_SUBSTRING(pool_id,1,20) IN 
        (0x11f0b5cca01b0f0a9fe6265ad6e8ee3419c68440,
        0xd20f6f1d8a675cdca155cb07b5dc9042c467153f)
    
    UNION ALL   
    SELECT
      BYTEARRAY_SUBSTRING(pool_id,1,20) as pool_id,
      'ethereum' as blockchain,
      token_address AS token,
      normalized_weight AS weight
    FROM balancer_v2_ethereum.pools_tokens_weights
    WHERE BYTEARRAY_SUBSTRING(pool_id,1,20) IN 
        (0x5c6ee304399dbdb9c8ef030ab642b10820db8f56,
        0xf16aee6a71af1a9bc8f56975a4c2705ca7a782bc,
        0x9232a548dd9e81bac65500b5e0d918f8ba93675c,
        0xe91888a1d08e37598867d213a4acb5692071bb3a,
        0x39eb558131e5ebeb9f76a6cbf6898f6e6dce5e4e,
        0xe91888a1d08e37598867d213a4acb5692071bb3a,
        0xcb0e14e96f2cefa8550ad8e4aea344f211e5061d,
        0xdf2c03c12442c7a0895455a48569b889079ca52a,
        0x26cc136e9b8fd65466f193a8e5710661ed9a9827,
        0x158e0fbc2271e1dcebadd365a22e2b4dd173c0db,
        0xd689abc77b82803f22c49de5c8a0049cc74d11fd,
        0xcf7b51ce5755513d4be016b0e28d6edeffa1d52a,
        0x02ca8086498552c071451724d3a34caa3922b65a,
        0x3de27efa2f1aa663ae5d458857e731c129069f29)
        
    UNION ALL   
    SELECT
      BYTEARRAY_SUBSTRING(pool_id,1,20) as pool_id,
      'ethereum' as blockchain,
      token_address AS token,
      normalized_weight AS weight
    FROM balancer_v1_ethereum.pools_tokens_weights
    WHERE BYTEARRAY_SUBSTRING(pool_id,1,20) IN 
        (0xc697051d1c6296c24ae3bcef39aca743861d9a81)
    ),
  
  beginning_token_price AS (
    SELECT
      tw.pool_id,
      tw.blockchain,
      tw.token,
      tw.weight,
      p.price
    FROM tokens_and_weight AS tw
    LEFT JOIN prices AS p ON tw.token = p.token AND tw.blockchain = p.blockchain 
    WHERE
      p.day = DATE_TRUNC('day', CURRENT_DATE - INTERVAL '1' month + INTERVAL '1' day)
     ),
      
  delta AS (
    SELECT
      p.day,
      tw.pool_id,
      tw.blockchain,
      p.token,
      p.price / CAST(btp.price AS DOUBLE) AS delta,
      btp.weight
    FROM tokens_and_weight AS tw
    INNER JOIN prices AS p ON tw.token = p.token AND tw.blockchain = p.blockchain 
    INNER JOIN beginning_token_price AS btp ON tw.token = btp.token AND tw.blockchain = btp.blockchain 
        AND tw.pool_id = btp.pool_id
  ),
  
  average_delta AS (
    SELECT
      day,
      pool_id,
      blockchain,
      token,
      weight,
      AVG(delta) AS delta
    FROM delta
    GROUP BY 1, 2, 3, 4, 5),
    
  delta_functions AS (
    SELECT
      day,
      pool_id,
      blockchain,
      SUM(delta * weight) AS hold_value_usd,
      EXP(SUM(LN(POWER(delta, weight)))) AS pool_value_usd
    FROM average_delta
    GROUP BY 1, 2, 3),
    
  impermanent_loss_by_day AS (
    SELECT
      day,
      pool_id,
      blockchain,
      (pool_value_usd / CAST(hold_value_usd AS DOUBLE)) - 1 AS impermanent_loss
    FROM delta_functions
  )
  
SELECT
  day,
  pool_id,
  blockchain,
  CASE WHEN impermanent_loss < 1 THEN impermanent_loss
    ELSE 0 
    END AS impermanent_loss
FROM impermanent_loss_by_day
    WHERE day = CURRENT_DATE