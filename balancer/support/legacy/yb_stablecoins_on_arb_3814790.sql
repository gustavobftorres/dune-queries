-- part of a query repo
-- query name: YB stablecoins on arb
-- query link: https://dune.com/queries/3814790


SELECT 
    blockchain,
    token_address,
    token_symbol,
    decimals
FROM gyroscope.gyro_tokens
WHERE blockchain = 'arbitrum'

UNION ALL

SELECT *
FROM (VALUES
    ('arbitrum', 0xe3b3fe7bca19ca77ad877a5bebab186becfad906, 'sFRAX', 18),
    ('arbitrum', 0xbC404429558292eE2D769E57d57D6E74bbd2792d, 'sUSX', 18),
    ('arbitrum', 0xd3443ee1e91aF28e5FB858Fbd0D72A63bA8046E0, 'gUSDC', 6)
) AS tokens(blockchain, token_address, token_symbol, decimals)