-- part of a query repo
-- query name: humpyWallets
-- query link: https://dune.com/queries/3859543


WITH providers as (
    SELECT * FROM (values
    (0x1e7267fa2628d66538822fc44f0edb62b07272a4, 'Humpy1'),
    (0xc407e861f5a16256534b0c92fdd8220a35831840, 'Humpy2'),
    (0x42d1f0e3e5461e368f24a29f68b212c1f15036a5, 'Humpy3'),
    (0xba55e4e6fca6abd74071516d0784aced07adef47, 'Humpy4'),
    (0xae0baf66e8f5bb87a6fd54066e469cdfe93212ec, 'Humpy5'),
    (0x8a8743afc23769d5b27fb22af510da3147bb9a55, 'Humpy6'),
    (0xd23da72ee782227074851e1090bff19f6b15e978, 'Humpy7'),
    (0x014e61311e4dd2364cf6c0868c9978c5887deca8, 'Humpy8'),
    (0xc0a893145ad461af44241a7db5bb99b8998e7d2c, 'Humpy9'),
    (0x36cc7b13029b5dee4034745fb4f24034f3f2ffc6, 'HumpyisGOLD'),
    (0x9e9f535da358bf4f9cdc10a3d690dcf981956f68, 'Humpy11'),
    (0x50ecfcc005a10537ffacf02466690a0d00145e47, 'Humpy12'),
    (0x90be067d924d86d4e2cfbdf7b1698a1f665cb61b, 'Humpy13'),
    (0x8b781a032c0ff967d2786a66afb1dbd5128fc382, 'Humpy14'),
    (0x4086e3e1e99a563989a9390facff553a4f29b6ee, 'Humpy15'),
    (0x5e1dfa4892095400f374c78320653d338d0cec8b, 'Humpy16'),
    (0xfa2d501dea12306281b9fbccc93bcbbe38ef0a4e, 'Humpy17'),
    (0x45ec6a657ad37845dc1d2c3ac653721d3037bf41, 'Humpy18'),
    (0x11eef043a0908f209ce53ac289a0fec2e82ce45d, 'Humpy19'),
    (0x0a7b24cca8e5b9379535649493fee3c2b0f61dc5, 'Humpy20'),
    (0x742ebaadad05b33bb2613e2aa4458304f9b85383, 'HumpyDumpy'),
    (0x5f547f19e5ad75bc19196d2f0472a2f4522fefd6, 'Humpy21'),
    (0x79ff36919d24f004d68428a46b9cb08640c0b23b, 'Humpy22'),
    (0x9e38d938e909e15c155e35fc2dbb9008a1d019af, 'Humpy23'),
    (0xe09697e1e476dce90b1199b0c5aa73bd33589a60, 'Humpy24'),
    (0x90be067d924d86d4e2cfbdf7b1698a1f665cb61b, 'Humpy25'),
    (0x2b67a3c0b90f6ae4394210692f69968d02970126, 'Humpy26'),
    (0x6e139b850f67bcdfd4191f40fd8d55eed1edb191, 'Humpy27'),
    (0xc4dc891d5b5171f789829d6050d5eb64c447e0fe, 'Humpy28'),
    (0xd519d5704b41511951c8cf9f65fee9ab9bef2611, 'Humpy29'),
    (0x19ae63358648795aaf29e36733f04fcef683aa69, 'Humpy30'),
    (0xba5c2f2165ddd691f99e12a23ec75cc1519930b4, 'Humpy31')
    ) AS t(wallet_address, provider)
)

SELECT * FROM providers
