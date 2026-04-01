-- part of a query repo
-- query name: (query_2478528) balancer_sources_ethereum
-- query link: https://dune.com/queries/2478528


WITH arbitrage_labels as
(
SELECT
    DISTINCT(t1.tx_to) as address,
    'Arbitrage Bot' as name,
    t1.blockchain
FROM dex.trades t1
INNER JOIN dex.trades t2
ON t1.tx_hash = t2.tx_hash AND t1.token_bought_address = t2.token_sold_address
AND t1.token_sold_address = t2.token_bought_address
AND t1.blockchain = t2.blockchain
AND (t1.project = 'balancer' AND t2.project != 'balancer' 
OR t2.project = 'balancer' AND t1.project != 'balancer')
WHERE t1.blockchain = 'ethereum'
AND t1.tx_hash NOT IN (SELECT DISTINCT tx_hash FROM dex_aggregator.trades WHERE blockchain = 'ethereum')
),

routers as (
SELECT * FROM (values
(0x1291C02D288de3De7dC25353459489073D11E1Ae, '0x'),
(0xdef1c0ded9bec7f1a1670819833240f027b25eff, '0x'),
(0x61935cbdd02287b511119ddb11aeb42f1593b7ef, '0x'),
(0xe66b31678d6c16e9ebf358268a790b763c133750, '0x'),
(0x8331f9acce69b02c281f40a00706f758665cce77, '0x'),
(0x111111125434b319222cdbf8c261674adb56f3ae, '1inch'),
(0x1111111254760F7ab3F16433eea9304126DCd199, '1inch'),
(0x1111111254fb6c44bac0bed2854e76f90643097d, '1inch'),
(0x1111111254eeb25477b68fb85ed929f73a960582, '1inch'),
(0xad3b67BCA8935Cb510C8D18bD45F0b94F54A968f, '1inch'),
(0x11111254369792b2ca5d084ab5eea397ca8fa48b, '1inch'),
(0x11111112542d85b3ef69ae05771c2dccff4faa26, '1inch'),
(0xa88800cd213da5ae406ce248380802bd53b47647, '1inch'),
(0x0B8a49d816Cc709B6Eadb09498030AE3416b66Dc, '1inch'),
(0x165d98de005d2818176b99b1a93b9325dbe58181, '1inch'),
(0x1e9d349cec77fea6481f009593101d0e20a69490, '1inch'),
(0x111111125434b319222cdbf8c261674adb56f3ae, '1inch'),
(0x111111125421cA6dc452d289314280a0f8842A65, '1inch'),
(0xad3b67BCA8935Cb510C8D18bD45F0b94F54A968f, '1inch'),
(0x872fbcb1b582e8cd0d0dd4327fbfa0b4c2730995, 'Aave'),
(0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9, 'Aave'),
(0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2, 'Aave'),
(0xADC0A53095A0af87F3aa29FE0715B5c28016364e, 'Aave'),
(0x9799b475dec92bd99bbdd943013325c36157f383, 'Bancor'),
(0xBeb09beB09e95E6FEBf0d6EEb1d0D46d1013CC3C, 'Bebop'),
(0x9008d19f58aabd9ed0d60971565aa8510560ab41, 'CoWSwap'),
(0x663DC15D3C1aC63ff12E45Ab68FeA3F0a883C251, 'deBridge'),
(0x50f9bDe1c76bba997a5d6e7FEFff695ec8536194, 'DODO'),
(0xa356867fdcea8e71aeaf87805808803806231fdc, 'DODO'),
(0x83f0c6046E6d269950a0FfDC11477b4b69b17738, 'DSProxy'),
(0x3e66b66fd1d0b02fda6c811da9e0547970db2f21, 'router'),
(0x6317c5e82a06e1d8bf200d21f4510ac2c038ac81, 'Direct Router'),
(0xe0C38b2a8D09aAD53f1C67734B9A95E43d5981c0, 'FireBird'),
(0x324c5dc1fc42c7a4d43d92df1eba58a54d13bf2d, 'FluidVault'),
(0x69592e6f9d21989a043646fe8225da2600e5a0f7, 'Gravity'),
(0x3DD5A8007AdF2AAB06B13d9E3Be3574E216286ED, 'Gearbox'),
(0xdf2c270f610dc35d8ffda5b453e74db5471e126b, 'Gnosis Safe'),
(0x375f6b0cd12b34dc28e34c26853a37012c24dde5, 'Gnosis Safe'),
(0x40a2accbd92bca938b02010e17a5b8929b49130d, 'Gnosis Safe'),
(0xdafca7a5e3b67b8f36c1fdd7691ed85bbb54cc18, 'Gnosis Safe'),
(0x9d5df30f475cea915b1ed4c0cca59255c897b61b, 'Gnosis Safe'),
(0xf6eb041840d35e64349b3f1ec990afc1fa99a133, 'Gnosis Safe'),
(0xb1748c79709f4ba2dd82834b8c82d4a505003f27, 'Gnosis Safe'),
(0x997d1ed51ff7389883913311810176cbdbd5d1d5, 'Gnosis Safe'),
(0x1da05bce1edd2369cddcb35e747859ef6a675010, 'Gnosis Safe'),
(0x9143de83a0c26081f60773f1bcda4c619bbb7ab6, 'Gnosis Safe'),
(0xa976ea51b9ba3232706af125a92e32788dc08ddc, 'Gnosis Safe'),
(0xfdd86a96f47015d9c457c841e1d52d06ede16a92, 'Gnosis Safe'),
(0x8991d9deb0cb6ad83acc5f2b733c297879b2424b, 'Gnosis Safe'),
(0x8adc75aa643cd5d804bab300a2d5becd9a9e6b8b, 'Gnosis Safe'),
(0x6131b5fae19ea4f9d964eac0408e4408b66337b5, 'Kyber'),
(0xdf1a1b60f2d438842916c0adc43748768353ec25, 'Kyber'),
(0x00555513acf282b42882420e5e5ba87b44d8fa6e, 'Kyber'),
(0x617dee16b86534a5d792a4d7a62fb491b544111e, 'Kyber'),
(0xdcdbf71a870cc60c6f9b621e28a7d3ffd6dd4965, 'Lido'),
(0x89c6340b1a1f4b25d36cd8b063d49045caf3f818, 'Li.Fi'),
(0xa6e941eab67569ca4522f70d343714ff51d571c4, 'Magpie'),
(0xcb1b068cb3937feaa2106d430af49681982626bc, 'Maker'),
(0x95d69f35547f169639e0f5969b746d9b634d3571, 'Maker'),
(0x4000235a519e9728a9aada6872cb8f152b7abe47, 'Maker'),
(0x28c9f3117921264b646a573df96ebf6ef543c1d5, 'Maker'),
(0x8e675cd61b33edeb425cad0bbe7edd06324faeef, 'Maker'),
(0xf002dcdd08a0d1879901956f1b4de227ebb44678, 'Maker'),
(0x80870b156ff0a8508e22b879d4e157d1dfa028ab, 'Maker'),
(0x340ce954128bc6501bebbb36c91540cbc8ace156, 'Maker'),
(0x0235639c9b6de98f087d9ef91847df80b5de622f, 'Maker'),
(0xaa5c792a41bb51e118583b9febb7020040347f87, 'Maker'),
(0xbdb9509587a6e13ba5b8eff68a6b7cb318e47809, 'Maker'),
(0xc00d1fe4c3db287b0a1ee1aa5b7435b4ad09a339, 'Maker'),
(0xfb1f953bc6cc9b05a0e232bb8c8b8f251c90aa1c, 'Maker'),
(0x246f871c8cd4aab2aea12ef91017d64f0df32ebf, 'Maker'),
(0xf91e8444e465dd0216c9c53e505ab20eb61acbfb, 'Maker'),
(0x881d40237659c251811cec9c364ef91dc08d300c, 'Metamask'),
(0x76f4eed9fe41262669d0250b2a97db79712ad855, 'Odos'),
(0xCf5540fFFCdC3d510B18bFcA6d2b9987b0772559, 'Odos'),
(0x0d05a7d3448512b78fa8a9e46c4872c88c4a0d05, 'Odos'),
(0x26d26b1a0243566d1cd38ff9afd5fd3f0fb6cbb4, 'Open Ocean'),
(0x6352a56caadc4f1e25cd6c75970fa768a3304e64, 'Open Ocean'),
(0xdef171fe48cf0115b1d80b88dc8eab59176fee57, 'Paraswap'),
(0x1bd435f3c054b6e901b7b108a0ab7617c808677b, 'Paraswap'),
(0x9509665d015bfe3c77aa5ad6ca20c8afa1d98989, 'Paraswap'),
(0x135896de8421be2ec868e0b811006171d9df802a, 'Paraswap'),
(0x6A000F20005980200259B80c5102003040001068, 'Paraswap'),
(0x0000000000bbF5c5Fd284e657F01Bd000933C96D, 'Paraswap'),
(0xd7852E139a7097E119623de0751AE53a61efb442, 'Paraswap'),
(0x00000000005bbb0ef59571e58418f9a4357b68a0, 'Pendle'),
(0xBBbfD134E9b44BfB5123898BA36b01dE7ab93d98, 'Relay'),
(0x16d5a408e807db8ef7c578279beeee6b228f1c1c, 'RocketRouter'),
(0x7bc735d6974f6153b1de24e40c0bf5715ca7fe1d, 'Sushi'),
(0x4d66839b42569b12002f18ec865f43f91ba5e2c3, 'Sushi'),
(0x110492b31c59716ac47337e616804e3e3adc0b4a, 'Sushi'),
(0x50044ef9d7ef2d4fe9ee36b2a371b2545172b05c, 'Sushi'),
(0x615687f0ac866a61df6550a17812c71d2635bd91, 'Sushi'),
(0x055475920a8c93cffb64d039a8205f7acc7722d3, 'Sushi'),
(0x03f34be1bf910116595db1b11e9d1b2ca5d59659, 'Tokenlon'),
(0xb20bd5d04be54f870d5c0d3ca85d82b34b836405, 'Uniswap V2'),
(0x755c1a8f71f4210cd7b60b9439451efcbeba33d1, 'Uniswap V2'),
(0xc2adda861f89bbb333c90c492cb837741916a225, 'Uniswap V2'),
(0xcc3d1ecef1f9fd25599dbea2755019dc09db3c54, 'Uniswap V2'),
(0x27fd0857f0ef224097001e87e61026e39e1b04d1, 'Uniswap V2'),
(0xc6f348dd3b91a56d117ec0071c1e9b83c0996de4, 'Uniswap V2'),
(0xe6f19dab7d43317344282f803f8e8d240708174a, 'Uniswap V2'),
(0xae461ca67b15dc8dc81ce7615e0320da1a9ab8d5, 'Uniswap V2'),
(0xcd7989894bc033581532d2cd88da5db0a4b12859, 'Uniswap V2'),
(0xc0a6bb3d31bb63033176edba7c48542d6b4e406d, 'Uniswap V2'),
(0x343fd171caf4f0287ae6b87d75a8964dc44516ab, 'Uniswap V2'),
(0xa70d458a4d9bc0e6571565faee18a48da5c0d593, 'Uniswap V2'),
(0xb6909b960dbbe7392d405429eb2b3649752b4838, 'Uniswap V2'),
(0xcffdded873554f362ac02f8fb1f02e5ada10516f, 'Uniswap V2'),
(0x3041cbd36888becc7bbcbc0045e3b1f144466f5f, 'Uniswap V2'),
(0xca9c4cc09e901f4d2aa072ed1aa95dcbe3a7a8e5, 'Uniswap V2'),
(0x3a1b4f6dce585ef469a5daa73a6eb87ce13e859d, 'Uniswap V2'),
(0x6000da47483062A0D734Ba3dc7576Ce6A0B645C4, 'Uniswap X'),
(0xba12222222228d8ba445958a75a0704d566bf2c8, 'Vault'),
(0x99a58482bd75cbab83b27ec03ca68ff489b5788f, 'Curve'),
(0xf0d4c12a5768d806021f80a262b4d39d26c58b8d, 'Curve'),
(0x0000006daea1723962647b7e189d311d757fb793, 'wintermute'),
(0x136f1EFcC3f8f88516B9E94110D56FDBfB1778d1, 'Direct Router'),
(0x9179C06629ef7f17Cb5759F501D89997FE0E7b45, 'Direct Router'),
(0x1CD776897ef4f647bf8241Ec69549e4A9cb1D608, 'Direct Router'),
(0x5C6fb490BDFD3246EB0bB062c168DeCAF4bD9FDd, 'Direct Router'),
(0xedeafdef0901ef74ee28c207be8424d3b353d97a, 'Odos')
)
    as t (address, name))
    
SELECT al.* FROM arbitrage_labels al
LEFT JOIN routers r ON al.address = r.address
WHERE r.address IS NULL
UNION ALL
SELECT address, name, 'ethereum' as blockchain FROM routers
