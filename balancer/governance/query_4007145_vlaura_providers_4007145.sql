-- part of a query repo
-- query name: (query_ 4007145) vlAURA_providers
-- query link: https://dune.com/queries/4007145


WITH providers as (
    SELECT * FROM (values
    (0xd14f076044414c255d2e82cceb1cb00fb1bba64c, 'AuraMaxi'),
    (0x36cc7b13029b5dee4034745fb4f24034f3f2ffc6, 'Humpy'),
    (0x8a8743afc23769d5b27fb22af510da3147bb9a55, 'Humpy'),
    (0x9e9f535da358bf4f9cdc10a3d690dcf981956f68, 'Humpy'),
    (0xc407e861f5a16256534b0c92fdd8220a35831840, 'Humpy'),
    (0xc0a893145ad461af44241a7db5bb99b8998e7d2c, 'Humpy'),
    (0xae0baf66e8f5bb87a6fd54066e469cdfe93212ec, 'Humpy'),
    (0x014e61311e4dd2364cf6c0868c9978c5887deca8, 'Humpy'),
    (0x1e7267fa2628d66538822fc44f0edb62b07272a4, 'Humpy'),
    (0x8b781a032c0ff967d2786a66afb1dbd5128fc382, 'Humpy'),
    (0x5f547f19e5ad75bc19196d2f0472a2f4522fefd6, 'Humpy'),
    (0x42d1f0e3e5461e368f24a29f68b212c1f15036a5, 'Humpy'),
    (0xba55e4e6fca6abd74071516d0784aced07adef47, 'Humpy'),
    (0xd23da72ee782227074851e1090bff19f6b15e978, 'Humpy'),
    (0x79ff36919d24f004d68428a46b9cb08640c0b23b, 'Humpy'),
    (0x9e38d938e909e15c155e35fc2dbb9008a1d019af, 'Humpy'),
    (0xe09697e1e476dce90b1199b0c5aa73bd33589a60, 'Humpy'),
    (0x50ecfcc005a10537ffacf02466690a0d00145e47, 'Humpy'),
    (0x90be067d924d86d4e2cfbdf7b1698a1f665cb61b, 'Humpy'),
    (0x90be067d924d86d4e2cfbdf7b1698a1f665cb61b, 'Humpy'),
    (0x4086e3e1e99a563989a9390facff553a4f29b6ee, 'Humpy'),
    (0x2b67a3c0b90f6ae4394210692f69968d02970126, 'Humpy'),
    (0x6e139b850f67bcdfd4191f40fd8d55eed1edb191, 'Humpy'),
    (0xc4dc891d5b5171f789829d6050d5eb64c447e0fe, 'Humpy'),
    (0x5e1dfa4892095400f374c78320653d338d0cec8b, 'Humpy'),
    (0xfa2d501dea12306281b9fbccc93bcbbe38ef0a4e, 'Humpy'),
    (0x45ec6a657ad37845dc1d2c3ac653721d3037bf41, 'Humpy'),
    (0x11eef043a0908f209ce53ac289a0fec2e82ce45d, 'Humpy'),
    (0x0a7b24cca8e5b9379535649493fee3c2b0f61dc5, 'Humpy'),
    (0xd519d5704b41511951c8cf9f65fee9ab9bef2611, 'Humpy'),
    (0x19ae63358648795aaf29e36733f04fcef683aa69, 'Humpy'),
    (0xba5c2f2165ddd691f99e12a23ec75cc1519930b4, 'Humpy'),
    (0x7629fc134e5a7febef6340438d96881c8d121f2c, 'Jones DAO'),
    (0x52ea58f4fc3ced48fa18e909226c1f8a0ef887dc, 'Stake DAO'),
    (0xfc78f8e1Af80A3bF5A1783BB59eD2d1b10f78cA9, 'Aura'), -- Treasury Multisig
    (0x10a19e7ee7d7f8a52822f6817de8ea18204f2e4f, 'Balancer'), -- DAO multisig
    (0x9a5BDF08a6969A4bDb7724beE3c6d8964BDc0B28, 'Balancer'), -- Managed by Maxis
    (0xB1f881f47baB744E7283851bC090bAA626df931d, 'meditator29367.eth'),
    (0x849d52316331967b6ff1198e5e32a0eb168d039d, 'Gnosis DAO'),
    (0xca86d57519dbfe34a25eef0923b259ab07986b71, 'incoom.eth'),
    (0x285b7eea81a5b66b62e7276a24c1e0f83f7409c1, '0xmaki.eth'),
    (0x7b90e043aac79adea0dbb0690e3c832757207a3b, 'Paladin'),
    (0x4a266739e40664e80470cc335120a2a1fa0b3f3f, 'Mimo'),
    (0x9e2b6378ee8ad2a4a95fe481d63caba8fb0ebbf9, 'Alchemix'),
    (0x65bb797c2b9830d891d87288f029ed8dacc19705, 'Stargate'),
    (0x71e47a4429d35827e0312aa13162197c23287546, 'Threshold'),
    (0xa52fd396891e7a74b641a2cb1a6999fcf56b077e, 'Dinero'),
    (0x9d5df30f475cea915b1ed4c0cca59255c897b61b, 'Inverse'),
    (0xea06e1b4259730724885a39ce3ca670efb020e26, 'Beethoven'),
    (0x6665e62ef6f6db29d5f8191fbac472222c2cc80f, 'Defi Collective'),
    (0xc47ec74a753acb09e4679979afc428cde0209639, 'Spiral'),
    (0xb95a4779ccedc53010ef0df8bf8ed6aeb0e8c2b2, 'Paladin'),
    (0x205e795336610f5131be52f09218af19f0f3ec60, 'Aave'),
    (0x2ca74be68f0a0e053f030d143c1376806babedc9, 'Jarvis')
    ) AS t(wallet_address, provider)
)

SELECT * FROM providers
