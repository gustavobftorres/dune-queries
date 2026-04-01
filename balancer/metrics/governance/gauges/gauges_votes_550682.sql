-- part of a query repo
-- query name: Gauges Votes
-- query link: https://dune.com/queries/550682


WITH labels AS (
        SELECT '\x79eF6103A513951a3b25743DB509E267685726B7'::bytea AS gauge, 'B-rETH-STABLE' AS symbol
        UNION ALL
        SELECT '\x5A481455E62D5825429C8c416f3B8D2938755B64'::bytea AS gauge, '80D2D-20USDC' AS symbol
        UNION ALL
        SELECT '\x34f33CDaED8ba0E1CEECE80e5f4a73bcf234cfac'::bytea AS gauge, 'staBAL3' AS symbol
        UNION ALL
        SELECT '\x68d019f64A7aa97e2D4e7363AEE42251D08124Fb'::bytea AS gauge, 'bb-a-USD' AS symbol
        UNION ALL
        SELECT '\x055d483D00b0FFe0c1123c96363889Fb03fa13a4'::bytea AS gauge, 'B-50VITA-50WETH' AS symbol
        UNION ALL
        SELECT '\xaB5ea78c8323212cC5736bfe4874557Bc778Bfbf'::bytea AS gauge, 'LPePyvWBTC-29APR22' AS symbol
        UNION ALL
        SELECT '\xC5f8B1de80145e3a74524a3d1a772a31eD2B50cc'::bytea AS gauge, 'NWWP' AS symbol
        UNION ALL
        SELECT '\x78259f2e946B11a0bE404d29d3cc017eCddE84C6'::bytea AS gauge, 'LPePyvDAI-29APR22' AS symbol
        UNION ALL
        SELECT '\x5F4d57fd9Ca75625e4B7520c71c02948A48595d0'::bytea AS gauge, 'B-50WETH-50YFI' AS symbol
        UNION ALL
        SELECT '\xdB7D7C535B4081Bb8B719237bdb7DB9f23Cc0b83'::bytea AS gauge, 'B-50WETH-50USDT' AS symbol
        UNION ALL
        SELECT '\xD61dc7452C852B866c0Ae49F4e87C38884AE231d'::bytea AS gauge, '50Silo-50WETH' AS symbol
        UNION ALL
        SELECT '\xAFc28B2412B343574E8673D4fb6b220473677602'::bytea AS gauge, 'B-50COMP-50WETH' AS symbol
        UNION ALL
        SELECT '\x40AC67ea5bD1215D99244651CC71a03468bce6c0'::bytea AS gauge, 'sNOTE-BPT' AS symbol
        UNION ALL
        SELECT '\x4ca6AC0509E6381Ca7CD872a6cdC0Fbf00600Fa1'::bytea AS gauge, 'B-60WETH-40DAI' AS symbol
        UNION ALL
        SELECT '\xbD0DAe90cb4a0e08f1101929C2A01eB165045660'::bytea AS gauge, '20WETH-80FDT' AS symbol
        UNION ALL
        SELECT '\x31e7F53D27BFB324656FACAa69Fe440169522E1C'::bytea AS gauge, 'B-50LINK-50WETH' AS symbol
        UNION ALL
        SELECT '\x9AB7B0C7b154f626451c9e8a68dC04f58fb6e5Ce'::bytea AS gauge, 'B-50USDC-50WETH' AS symbol
        UNION ALL
        SELECT '\x605eA53472A496c3d483869Fe8F355c12E861e19'::bytea AS gauge, 'B-50SNX-50WETH' AS symbol
        UNION ALL
        SELECT '\x78DF155d6d75Ca2a1b1B2027f37414Ac1e7A1Ed8'::bytea AS gauge, 'LPePyvUSDC-29APR22' AS symbol
        UNION ALL
        SELECT '\xbeC2d02008Dc64A6AD519471048CF3D3aF5ca0C5'::bytea AS gauge, 'mBPT' AS symbol
        UNION ALL
        SELECT '\xf4339872Ad09B34a29Be76EE81D4F30BCf7dbf9F'::bytea AS gauge, 'BPTUMAUSDC' AS symbol
        UNION ALL
        SELECT '\x4f9463405F5bC7b4C1304222c1dF76EFbD81a407'::bytea AS gauge, 'B-30FEI-70WETH' AS symbol
        UNION ALL
        SELECT '\xD6E4d70bdA78FBa018c2429e1b84153b9284298e'::bytea AS gauge, 'B-50REN-50WETH' AS symbol
        UNION ALL
        SELECT '\x942CB1Ed80D3FF8028B3DD726e0E2A9671bc6202'::bytea AS gauge, 'B-80LDO-20WETH' AS symbol
        UNION ALL
        SELECT '\xa57453737849A4029325dfAb3F6034656644E104'::bytea AS gauge, '20WETH-80HAUS' AS symbol
        UNION ALL
        SELECT '\xb154d9D7f6C5d618c08D276f94239c03CFBF4575'::bytea AS gauge, 'VBPT' AS symbol
        UNION ALL
        SELECT '\xcD4722B7c24C29e0413BDCd9e51404B4539D14aE'::bytea AS gauge, 'B-stETH-STABLE' AS symbol
        UNION ALL
        SELECT '\x158772F59Fe0d3b75805fC11139b46CBc89F70e5'::bytea AS gauge, '50COW-50WETH' AS symbol
        UNION ALL
        SELECT '\x4e311e207CEAaaed421F17E909DA16527565Daef'::bytea AS gauge, 'B-50MATIC-50WETH' AS symbol
        UNION ALL
        SELECT '\xc43d32BC349cea7e0fe829F53E26096c184756fa'::bytea AS gauge, '50N/A-50N/A' AS symbol
        UNION ALL
        SELECT '\x8F4a5C19A74D7111bC0e1486640F0aAB537dE5A1'::bytea AS gauge, 'B-80UNN-20WETH' AS symbol
        UNION ALL
        SELECT '\xCB664132622f29943f67FA56CCfD1e24CC8B4995'::bytea AS gauge, 'B-80GNO-20WETH' AS symbol
        UNION ALL
        SELECT '\x852CF729dEF9beB9De2f18c97a0ea6bf93a7dF8B'::bytea AS gauge, '50OHM-25DAI-25WETH' AS symbol
        UNION ALL
        SELECT '\x5E7B7B41377Ce4B76d6008F7a91ff9346551c853'::bytea AS gauge, 'undefined' AS symbol
        UNION ALL
        SELECT '\xAF50825B010Ae4839Ac444f6c12D44b96819739B'::bytea AS gauge, '20WBTC-80BADGER' AS symbol
        UNION ALL
        SELECT '\x57d40FF4cF7441A04A05628911F57bb940B6C238'::bytea AS gauge, 'staBAL3-BTC' AS symbol
        UNION ALL
        SELECT '\xE190E5363C925513228Bf25E4633C8cca4809C9a'::bytea AS gauge, 'undefined' AS symbol
        UNION ALL
        SELECT '\x7A89f34E976285b7b885b32b2dE566389C2436a0'::bytea AS gauge, 'B-80BAL-20WETH' AS symbol
        UNION ALL
        SELECT '\x4E3c048BE671852277Ad6ce29Fd5207aA12fabff'::bytea AS gauge, 'B-50WBTC-50WETH' AS symbol
        UNION ALL
        SELECT '\xFBf87D2C22d1d298298ab5b0Ec957583a2731d15'::bytea AS gauge, 'BPSP' AS symbol
        UNION ALL
        SELECT '\xA80D514734e57691f45aF76bb44d1202858FD1F0'::bytea AS gauge, 'B-POLYDEFI' AS symbol
        UNION ALL
        SELECT '\xE0b50B0635b90F7021d2618f76AB9a31B92D0094'::bytea AS gauge, 'B-staBAL-3' AS symbol
        UNION ALL
        SELECT '\xC6FB8C72d3BD24fC4891C51c2cb3a13F49c11335'::bytea AS gauge, 'TELX-50TEL-50BAL' AS symbol
        UNION ALL
        SELECT '\xc3bB46B8196C3F188c6A373a6C4Fde792CA78653'::bytea AS gauge, 'B-stMATIC-STABLE' AS symbol
        UNION ALL
        SELECT '\xB0de49429fBb80c635432bbAD0B3965b28560177'::bytea AS gauge, 'VST-USDC-USDT-DAI-BSP' AS symbol
        UNION ALL
        SELECT '\x6D73Df7aFC4e0144DeC3BE083dFA3882E53c5BA5'::bytea AS gauge, '20USDC-80THX' AS symbol
        UNION ALL
        SELECT '\xAb6efd2882BB25c732Bf0A5f8d98BE752f0DdAAF'::bytea AS gauge, 'BAL-VISION-LP' AS symbol
        UNION ALL
        SELECT '\x359EA8618c405023Fc4B98dAb1B01F373792a126'::bytea AS gauge, 'B-33WETH-33WBTC-33USDC' AS symbol
        UNION ALL
        SELECT '\x3fDb6fB126521A28f06893F9629DA12f7B7266Eb'::bytea AS gauge, '80GMX-20WETH' AS symbol
        UNION ALL
        SELECT '\xb5ad7d6d6F92a77F47f98C28C84893FBccc94809'::bytea AS gauge, 'BPSP-TUSD' AS symbol
        UNION ALL
        SELECT '\xCBbd866835433C620059129aaf12EE9c59dbC0d7'::bytea AS gauge, 'B-33AVAX-33WETH-33SOL' AS symbol
        UNION ALL
        SELECT '\x6823DcA6D70061F2AE2AAA21661795A2294812bF'::bytea AS gauge, 'B-60BAL-40WETH' AS symbol
        UNION ALL
        SELECT '\x022A843fFeE5A6FE1646C980b94286ef0D05F759'::bytea AS gauge, '20WETH-80BANK' AS symbol
        UNION ALL
        SELECT '\xDaE03Cd2ec908710E98ffc5f4Ff540Fe2c5C1e17'::bytea AS gauge, '20WMATIC-80SAND' AS symbol
        UNION ALL
        SELECT '\x981Fb05B738e981aC532a99e77170ECb4Bc27AEF'::bytea AS gauge, 'B-80GNO-20WETH' AS symbol
        UNION ALL
        SELECT '\x05e7732bF9ae5592E6AA05aFE8Cd80f7Ab0a7bEA'::bytea AS gauge, 'B-80TCR-20WETH' AS symbol
        UNION ALL
        SELECT '\xA6359EB485d23412EB40f1F0Dbd80e1A4Fe87e6b'::bytea AS gauge, 'FRAX-UST-USDC-USDT-BSP' AS symbol
        UNION ALL
        SELECT '\xc77E5645Dbe48d54afC06655e39D3Fe17eB76C1c'::bytea AS gauge, '33DPX-33RDPX-33WETH' AS symbol
        UNION ALL
        SELECT '\x899F737750db562b88c1E412eE1902980D3a4844'::bytea AS gauge, 'B-80PICKLE-20WETH' AS symbol
        UNION ALL
        SELECT '\xA5A0B6598B90d214eAf4d7a6b72d5a89C3b9A72c'::bytea AS gauge, 'B-POLYBASE' AS symbol
        UNION ALL
        SELECT '\x785F08fB77ec934c01736E30546f87B4daccBe50'::bytea AS gauge, '80MAGIC-20WETH' AS symbol
        UNION ALL
        SELECT '\x435272180a4125f3B47c92826F482FC6cc165958'::bytea AS gauge, 'B-80LINK-20WETH' AS symbol
        UNION ALL
        SELECT '\x5A3970E3145Bbba4838D1a3A31C79bcD35A16A9E'::bytea AS gauge, 'B-POLYDEFI' AS symbol
        UNION ALL
        SELECT '\x45012035a728b0a9B344036e6bff6c775EE09769'::bytea AS gauge, 'B-50WETH-50USDC' AS symbol
        UNION ALL
        SELECT '\x88D07558470484c03d3bb44c3ECc36CAfCF43253'::bytea AS gauge, 'BPTC' AS symbol
        UNION ALL
        SELECT '\xF0ea3559Cf098455921d74173dA83fF2f6979495'::bytea AS gauge, 'MAI-BSP' AS symbol
        UNION ALL
        SELECT '\x397649FF00de6d90578144103768aaA929EF683d'::bytea AS gauge, 'TELX-60TEL-20BAL-20USDC' AS symbol
        UNION ALL
        SELECT '\x6cb1A77AB2e54d4560fda893E9c738ad770da0B0'::bytea AS gauge, '50VSTA-50WETH' AS symbol
        UNION ALL
        SELECT '\xf30dB0Ca4605e5115Df91B56BD299564dcA02666'::bytea AS gauge, 'MWP' AS symbol
        UNION ALL
        SELECT '\xA6468eca7633246Dcb24E5599681767D27d1F978'::bytea AS gauge, '50COW-50GNO' AS symbol
        UNION ALL
        SELECT '\xE273d4aCC555A245a80cB494E9E0dE5cD18Ed530'::bytea AS gauge, '20DAI-80TCR' AS symbol
        UNION ALL
        SELECT '\xE32080A12723e5b8f1b0cEd1F308FE2f9cF7e3c9'::bytea AS gauge, 'BP-MTA' AS symbol
        UNION ALL
        SELECT '\xd27cb689083e97847Dc91C64Efc91C4445d46D47'::bytea AS gauge, 'BP-BTC-SP' AS symbol
        UNION ALL
        SELECT '\x211C27a32E686659566C3CEe6035c2343D823aab'::bytea AS gauge, 'B-50WBTC-50WETH' AS symbol
        UNION ALL
        SELECT '\xe867ad0a48e8f815dc0cda2cdb275e0f163a480b'::bytea AS gauge, 'veBAL' AS symbol
    ),

    gauges AS (
        SELECT g.addr AS gauge, gauge_type, symbol
        FROM balancer."GaugeController_evt_NewGauge" g
        LEFT JOIN labels l
        ON l.gauge = g.addr
    ),
    
    calendar AS (
        SELECT generate_series('2022-04-07'::timestamptz, CURRENT_DATE, '1 week'::interval) AS start_date
    ),
    
    rounds_info AS (
        SELECT
            start_date,
            start_date + '7d' AS end_date,
            ROW_NUMBER() OVER (ORDER BY start_date) AS round_id
        FROM calendar
    ),
    
    votes_with_gaps AS (
        SELECT
            COALESCE(round_id, 1) AS round_id,
            LEAD(round_id::int, 1, 9999) OVER (PARTITION BY "user", gauge_addr ORDER BY round_id) AS next_round,
            "user" AS provider,
            gauge_addr AS gauge,
            weight / 1e4 AS weight
        FROM balancer."GaugeController_evt_VoteForGauge" v
        LEFT JOIN rounds_info r
        ON v.evt_block_time >= r.start_date
        AND v.evt_block_time < r.end_date
    ),
    
    running_votes AS (
        SELECT r.round_id, r.end_date, provider, gauge, weight
        FROM rounds_info r
        LEFT JOIN votes_with_gaps v
        ON v.round_id <= r.round_id
        AND r.round_id < v.next_round
        AND v.weight > 0
    ),
    
    vote_results AS (
        SELECT round_id, v.gauge, symbol, SUM(vebal * v.weight) AS votes 
        FROM running_votes v
        JOIN dune_user_generated.balancer_vebal_balance b
        ON b.provider = v.provider
        AND b.day = v.end_date - '1 day'::interval
        AND vebal > 0
        JOIN gauges l
        ON l.gauge = v.gauge
        GROUP BY 1, 2, 3
        ORDER BY 4 
    ),
    
    top_gauges AS (
        SELECT gauge, symbol
        FROM vote_results
        ORDER BY round_id DESC, votes DESC
        LIMIT 15
    )

SELECT
    round_id,
    COALESCE(t.gauge, 'Others') AS symbol,
    SUM(votes) AS votes
FROM vote_results v
LEFT JOIN top_gauges t
ON t.gauge = v.gauge
GROUP BY 1, 2
ORDER BY round_id DESC, votes DESC