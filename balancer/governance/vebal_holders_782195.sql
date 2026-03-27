-- part of a query repo
-- query name: veBAL Holders
-- query link: https://dune.com/queries/782195


WITH
  locked_at AS (
    SELECT
      wallet_address as provider,
      MIN(day) AS locked_at
    FROM
     --vebal_balances_day
      balancer_ethereum.vebal_balances_day
    WHERE
      vebal_balance > 0
    GROUP BY
      1
  ),
  info AS (
    SELECT
      wallet_address as provider,
      CASE
        WHEN wallet_address = 0xaf52695e1bb01a16d33d7194c28c42b10e0dbec2 THEN CONCAT ('Aura', ' (0x',
        LOWER(SUBSTRING(to_hex(wallet_address), 1, 4)),
        '...',
        LOWER(SUBSTRING(to_hex(wallet_address), 37, 4)),')')
        WHEN wallet_address = 0x9cc56fa7734da21ac88f6a816af10c5b898596ce THEN CONCAT ('Tetu', ' (0x',
        LOWER(SUBSTRING(to_hex(wallet_address), 1, 4)),
        '...',
        LOWER(SUBSTRING(to_hex(wallet_address), 37, 4)),')')
        WHEN wallet_address IN 
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
        0x11eef043a0908f209ce53ac289a0fec2e82ce45d, 0x0a7b24cca8e5b9379535649493fee3c2b0f61dc5) 
        THEN CONCAT ('Humpy', ' (0x',
        LOWER(SUBSTRING(to_hex(wallet_address), 1, 4)),
        '...',
        LOWER(SUBSTRING(to_hex(wallet_address), 37, 4)),')')
        WHEN wallet_address = 0xe8d4d93d9728bd673b0197673a230f62255c7846 THEN CONCAT ('Aave', ' (0x',
        LOWER(SUBSTRING(to_hex(wallet_address), 1, 4)),
        '...',
        LOWER(SUBSTRING(to_hex(wallet_address), 37, 4)),')')
        WHEN wallet_address = 0xea79d1a83da6db43a85942767c389fe0acf336a5 THEN CONCAT ('Stake DAO', ' (0x',
        LOWER(SUBSTRING(to_hex(wallet_address), 1, 4)),
        '...',
        LOWER(SUBSTRING(to_hex(wallet_address), 37, 4)),')')
        WHEN wallet_address = 0xf5307a74d1550739ef81c6488dc5c7a6a53e5ac2 THEN CONCAT ('Vita DAO', ' (0x',
        LOWER(SUBSTRING(to_hex(wallet_address), 1, 4)),
        '...',
        LOWER(SUBSTRING(to_hex(wallet_address), 37, 4)),')')
        WHEN wallet_address IN 
        (0xf094d5205197435da9268dfb0540e1bf3c1c970a, 0x8b5c657b1a31d9deb90a2d6da6966a2186d1800b,
        0x849d52316331967b6ff1198e5e32a0eb168d039d) THEN CONCAT ('Gnosis DAO', ' (0x',
        LOWER(SUBSTRING(to_hex(wallet_address), 1, 4)),
        '...',
        LOWER(SUBSTRING(to_hex(wallet_address), 37, 4)),')')
        ELSE
      CONCAT(
        '0x',
        LOWER(SUBSTRING(to_hex(wallet_address), 1, 4)),
        '...',
        LOWER(SUBSTRING(to_hex(wallet_address), 37, 4))
      ) 
      END AS provider_short,
      lock_time,
      bpt_balance,
      vebal_balance
    FROM
      --vebal_balances_day
      balancer_ethereum.vebal_balances_day
    WHERE
      day = CURRENT_DATE
    ORDER BY
      5 DESC NULLS FIRST
  ),
  ranking as (
    SELECT
      ROW_NUMBER() OVER (
        ORDER BY
          vebal_balance DESC NULLS FIRST
      ) AS ranking,
      CONCAT(
        '<a target="_blank" href="https://etherscan.io/address/0x',
        LOWER(to_hex(i.provider)),
        '">',
        provider_short,
        '↗</a>'
      ) AS provider_link,
      i.provider as provider,
      CAST(locked_at AS DATE) AS locked_at,
      bpt_balance /* lock_time / (365*86400/12) AS lock_time, */,
      vebal_balance,
      CONCAT(
        '<a target="_blank" href="https://dune.com/balancer/vebal-analysis?Provider_t9371a=0x',
        LOWER(to_hex(i.provider)),
        '">view stats</a>'
      ) AS stats
    FROM
      info AS i
      JOIN locked_at AS l ON l.provider = i.provider
      AND vebal_balance > 0
    )
    
SELECT * FROM ranking 
    WHERE bpt_balance > 1 AND 
      (
        '{{Provider}}' = 'All'
        OR CAST(provider AS VARCHAR) = '{{Provider}}'
      )