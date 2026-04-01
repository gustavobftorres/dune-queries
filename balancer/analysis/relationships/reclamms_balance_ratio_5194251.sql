-- part of a query repo
-- query name: ReCLAMMs Balance Ratio
-- query link: https://dune.com/queries/5194251


WITH reclamms AS (
   SELECT 
       pool_address,
       pool_name
   FROM (
       VALUES 
           (0x1A0cde11fD13E9E347088e4cDc00801997911A75, 'ReCLAMM #1'),
           (0xd9a8bd46fbB0BaC27aA1A99E64931d406e3bBb3F, 'ReCLAMM #2'),
           (0x6B54B954E53c3fBaf84B6b97377f3760C91DB847, 'ReCLAMM #3'),
           (0x785D9232cB7195A7ddBA3864f30B750FD7596faC, 'ReCLAMM #4'),
           (0x63B52EBA7e565CcEC991910Bd3482D01bA3Bf70d, 'ReCLAMM #5'),
           (0x7Dc81fb7e93cdde7754bff7f55428226bD9cEF7b, 'ReCLAMM #6'),
           (0xc46e6A1CB1910c916620Dc81C7fd8c38891E1904, 'ReCLAMM #7')
   ) AS pools(pool_address, pool_name)
),
pool_weights AS (
   SELECT 
       l.day,
       l.pool_address,
       r.pool_name,
       l.token_symbol,
       l.pool_liquidity_usd,
       l.pool_liquidity_usd / SUM(l.pool_liquidity_usd) OVER (PARTITION BY l.day, l.pool_address) as token_weight,
       SUM(l.pool_liquidity_usd) OVER (PARTITION BY l.day, l.pool_address) as total_pool_liquidity
   FROM balancer.liquidity l
   JOIN reclamms r ON l.pool_address = r.pool_address
   WHERE l.blockchain = 'base'
   AND l.version = '3'
),
balance_indicator AS (
   SELECT 
       day,
       pool_address,
       pool_name,
       total_pool_liquidity,
       -- For 2-token pools: multiply weights and scale by 4
       CASE 
           WHEN COUNT(*) = 2 THEN 
               EXP(SUM(LN(token_weight))) * 4
           ELSE NULL 
       END as balance_score
   FROM pool_weights
   WHERE token_weight > 0  -- Avoid log(0)
   GROUP BY day, pool_address, pool_name, total_pool_liquidity
)
SELECT 
   day,
   pool_name,
   balance_score,
   total_pool_liquidity
FROM balance_indicator
WHERE balance_score IS NOT NULL
ORDER BY day DESC, pool_name DESC
