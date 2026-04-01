-- part of a query repo
-- query name: ReCLAMMs Balance Deviation
-- query link: https://dune.com/queries/5194273


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
       l.pool_liquidity_usd / SUM(l.pool_liquidity_usd) OVER (PARTITION BY l.day, l.pool_address) as token_weight
   FROM balancer.liquidity l
   JOIN reclamms r ON l.pool_address = r.pool_address
   WHERE l.blockchain = 'base'
   AND l.version = '3'
),
balance_deviation AS (
   SELECT 
       day,
       pool_address,
       pool_name,
       -- WETH positive deviation, USDC negative deviation
       CASE 
           WHEN token_symbol = 'WETH' THEN (token_weight - 0.5) * 100
           WHEN token_symbol = 'USDC' THEN (0.5 - token_weight) * 100
           ELSE NULL
       END as deviation_pct
   FROM pool_weights
   WHERE token_symbol IN ('WETH', 'USDC')
)
SELECT 
   day,
   pool_name,
   deviation_pct
FROM balance_deviation
WHERE deviation_pct IS NOT NULL
ORDER BY day DESC, pool_name DESC
