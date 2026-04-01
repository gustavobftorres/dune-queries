-- part of a query repo
-- query name: veBAL balance changes
-- query link: https://dune.com/queries/1200356


WITH balances_today AS (
        SELECT *
        --vebal_balances_day
        FROM balancer_ethereum.vebal_balances_day
        WHERE day = date_trunc('day', now())
    ),
    balances_1d_ago AS (
        SELECT *
        --vebal_balances_day
        FROM balancer_ethereum.vebal_balances_day
        WHERE day = date_trunc('day', now()) - interval '1' day
    ),
    balances_7d_ago AS (
        SELECT *
        --vebal_balances_day
        FROM balancer_ethereum.vebal_balances_day
        WHERE day = date_trunc('day', now()) - interval '7' day
    )
SELECT 
    CASE
        WHEN a.wallet_address = 0xaf52695e1bb01a16d33d7194c28c42b10e0dbec2 THEN CONCAT ('Aura (',        
        '<a target="_blank" href="https://dune.com/balancer/vebal-analysis?Provider_tcf87d=0x', 
        LOWER(to_hex(a.wallet_address)), 
        '">0x',
        LOWER(to_hex(a.wallet_address)), 
        '↗</a>', ')') 
        WHEN a.wallet_address = 0x9cc56fa7734da21ac88f6a816af10c5b898596ce THEN CONCAT ('Tetu (',         
        '<a target="_blank" href="https://dune.com/balancer/vebal-analysis?Provider_tcf87d=0x', 
        LOWER(to_hex(a.wallet_address)), 
        '">0x',
        LOWER(to_hex(a.wallet_address)), 
        '↗</a>', ')') 
        WHEN a.wallet_address IN 
        (0x36cc7b13029b5dee4034745fb4f24034f3f2ffc6, 0x8a8743afc23769d5b27fb22af510da3147bb9a55, 
        0x9e9f535da358bf4f9cdc10a3d690dcf981956f68,0xc407e861f5a16256534b0c92fdd8220a35831840,
        0xc0a893145ad461af44241a7db5bb99b8998e7d2c, 0xae0baf66e8f5bb87a6fd54066e469cdfe93212ec,
        0x014e61311e4dd2364cf6c0868c9978c5887deca8,0x1e7267fa2628d66538822fc44f0edb62b07272a4,
        0x8b781a032c0ff967d2786a66afb1dbd5128fc382,0x5f547f19e5ad75bc19196d2f0472a2f4522fefd6,
        0x42d1f0e3e5461e368f24a29f68b212c1f15036a5, 0xba55e4e6fca6abd74071516d0784aced07adef47,
        0xd23da72ee782227074851e1090bff19f6b15e978, 0x79ff36919d24f004d68428a46b9cb08640c0b23b,
        0x9e38d938e909e15c155e35fc2dbb9008a1d019af, 0xe09697e1e476dce90b1199b0c5aa73bd33589a60,
        0x50ecfcc005a10537ffacf02466690a0d00145e47, 0x90be067d924d86d4e2cfbdf7b1698a1f665cb61b,
        0x90be067d924d86d4e2cfbdf7b1698a1f665cb61b, 0x4086e3e1e99a563989a9390facff553a4f29b6ee,
        0x2b67a3c0b90f6ae4394210692f69968d02970126, 0x6e139b850f67bcdfd4191f40fd8d55eed1edb191,
        0xc4dc891d5b5171f789829d6050d5eb64c447e0fe, 0x5e1dfa4892095400f374c78320653d338d0cec8b,
        0xfa2d501dea12306281b9fbccc93bcbbe38ef0a4e, 0x45ec6a657ad37845dc1d2c3ac653721d3037bf41,
        0x11eef043a0908f209ce53ac289a0fec2e82ce45d, 0x0a7b24cca8e5b9379535649493fee3c2b0f61dc5,
        0xd519d5704b41511951c8cf9f65fee9ab9bef2611, 0x19ae63358648795aaf29e36733f04fcef683aa69,
        0xba5c2f2165ddd691f99e12a23ec75cc1519930b4) 
        THEN CONCAT ('Humpy (',         
        '<a target="_blank" href="https://dune.com/balancer/vebal-analysis?Provider_tcf87d=0x', 
        LOWER(to_hex(a.wallet_address)), 
        '">0x',
        LOWER(to_hex(a.wallet_address)), 
        '↗</a>', ')') 
        WHEN a.wallet_address = 0xe8d4d93d9728bd673b0197673a230f62255c7846 THEN CONCAT ('Aave (',
        '<a target="_blank" href="https://dune.com/balancer/vebal-analysis?Provider_tcf87d=0x', 
        LOWER(to_hex(a.wallet_address)), 
        '">0x',
        LOWER(to_hex(a.wallet_address)), 
        '↗</a>', ')') 
        WHEN a.wallet_address = 0xea79d1a83da6db43a85942767c389fe0acf336a5 THEN CONCAT ('Stake DAO (',        
        '<a target="_blank" href="https://dune.com/balancer/vebal-analysis?Provider_tcf87d=0x', 
        LOWER(to_hex(a.wallet_address)), 
        '">0x',
        LOWER(to_hex(a.wallet_address)), 
        '↗</a>', ')') 
        WHEN a.wallet_address = 0xf5307a74d1550739ef81c6488dc5c7a6a53e5ac2 THEN CONCAT ('Vita DAO (',        
        '<a target="_blank" href="https://dune.com/balancer/vebal-analysis?Provider_tcf87d=0x', 
        LOWER(to_hex(a.wallet_address)), 
        '">0x',
        LOWER(to_hex(a.wallet_address)), 
        '↗</a>', ')') 
        WHEN a.wallet_address IN 
        (0xf094d5205197435da9268dfb0540e1bf3c1c970a, 0x8b5c657b1a31d9deb90a2d6da6966a2186d1800b,
        0x849d52316331967b6ff1198e5e32a0eb168d039d) THEN CONCAT ('Gnosis DAO (', 
        '<a target="_blank" href="https://dune.com/balancer/vebal-analysis?Provider_tcf87d=0x', 
        LOWER(to_hex(a.wallet_address)), 
        '">0x',
        LOWER(to_hex(a.wallet_address)), 
        '↗</a>', ')') 
        ELSE
        CONCAT(
        '<a target="_blank" href="https://dune.com/balancer/vebal-analysis?Provider_tcf87d=0x', 
        LOWER(to_hex(a.wallet_address)), 
        '">0x',
        LOWER(to_hex(a.wallet_address)), 
        '↗</a>') 
        END as provider, 
    (a.vebal_balance - COALESCE(b.vebal_balance, 0)) as "1d_change",
    (a.vebal_balance - COALESCE(c.vebal_balance, 0)) as "7d_change"
FROM balances_today AS a
LEFT JOIN balances_1d_ago AS b
on a.wallet_address = b.wallet_address
LEFT JOIN balances_7d_ago AS c
on a.wallet_address = c.wallet_address
WHERE /*((a.vebal_balance - COALESCE(b.vebal_balance, 0)) > 1 OR 
(a.vebal_balance - COALESCE(c.vebal_balance, 0)) > 1)*/
(a.vebal_balance - COALESCE(b.vebal_balance, 0) > 1 OR 
a.vebal_balance - COALESCE(b.vebal_balance, 0) < -1
OR a.vebal_balance - COALESCE(c.vebal_balance, 0) > 1 OR 
a.vebal_balance - COALESCE(c.vebal_balance, 0) < -1)
ORDER BY 3 DESC
