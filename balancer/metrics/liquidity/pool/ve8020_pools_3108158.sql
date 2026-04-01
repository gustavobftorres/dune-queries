-- part of a query repo
-- query name: ve8020 Pools
-- query link: https://dune.com/queries/3108158


WITH 
    ve8020_pools AS (
        SELECT * FROM (values
                ('0x32df62dc3aed2cd6224193052ce665dc18165841', 'Radiant (arb)', '80 RDNT/20 WETH', 'arbitrum'),
                ('0x569061e2d807881f4a33e1cbe1063bc614cb75a4', 'Y2K Finance', '80 Y2K/20 WETH', 'arbitrum'),    
                ('0x920ce9ec4c52e740ab4c3d36fb5454c274907ae5', 'Parifi', '80 PRF/20 WETH', 'arbitrum'),    
                ('0x39a5bfd5fe32026cd93d81859b4b38cea78d8220', 'Lumin', '80 LUMIN/20 rETH', 'arbitrum'),
                ('0x85ec6ae01624ae0d2a04d0ffaad3a25884c7d0f3', 'Overnight', '80 OVN/20 wUSD+', 'arbitrum'),
                ('0x3efd3e18504dc213188ed2b694f886a305a6e5ed', 'PepeGame', '80 PEG/20 WETH', 'arbitrum'),
                ('0xeb3e64ad9314d20bf943ac72fb69f272603f9cce', 'Synonym', '80 SYNO/20 WETH', 'arbitrum'),
                ('0xcf7b51ce5755513d4be016b0e28d6edeffa1d52a', 'Radiant (eth)', '80 RDNT/20 WETH', 'ethereum'),
                ('0xc697051d1c6296c24ae3bcef39aca743861d9a81', 'Aave (v1)', '80 AAVE/20 WETH ', 'ethereum'),
                ('0x5c6ee304399dbdb9c8ef030ab642b10820db8f56', 'Balancer', '80 BAL/20 WETH ', 'ethereum'),
                ('0xf16aee6a71af1a9bc8f56975a4c2705ca7a782bc', 'Alchemix ', '80 ALCX/20 WETH', 'ethereum'),
                ('0x57766212638c425e9cb0c6d6e1683dda369c0fff', 'OPAL ', '80 GEM/20 WETH', 'ethereum'),                
                ('0xd689abc77b82803f22c49de5c8a0049cc74d11fd', 'Unsheth ', '80 USH/20 unshETH', 'ethereum'),
                ('0x02ca8086498552c071451724d3a34caa3922b65a', 'Root ', '80 ROOT/20 unshETH', 'ethereum'),
                ('0x9232a548dd9e81bac65500b5e0d918f8ba93675c', 'Timeless', '80 LIT/20 WETH', 'ethereum'),
                ('0x26cc136e9b8fd65466f193a8e5710661ed9a9827', 'BetSwirl', '80 BETS/20 wstETH', 'ethereum'),
                ('0xdf2c03c12442c7a0895455a48569b889079ca52a', 'Archimedes', '80 ARCH/20 WETH', 'ethereum'),
                ('0x3de27efa2f1aa663ae5d458857e731c129069f29', 'Aave (v2)', '80 AAVE/20 WstETH', 'ethereum'),
                ('0x158e0fbc2271e1dcebadd365a22e2b4dd173c0db', 'Idle', '80 IDLE/20 USDC', 'ethereum'),
                ('0x39eb558131e5ebeb9f76a6cbf6898f6e6dce5e4e', 'QI DAO', '80 QI/20 WETH', 'ethereum'),                
                ('0xe91888a1d08e37598867d213a4acb5692071bb3a', 'Raft', '80 RAFT/20 R', 'ethereum'),
                ('0x577A7f7EE659Aa14Dc16FD384B3F8078E23F1920', 'VaultCraft', '80 VCX/20 WETH', 'ethereum'),
                ('0x7056c8dfa8182859ed0d4fb0ef0886fdf3d2edcf', 'Origin Ether', '80 OETH/20 WETH', 'ethereum'),
                ('0xcb0e14e96f2cefa8550ad8e4aea344f211e5061d', 'Paraswap (eth)', '80 PSP/20 WETH', 'ethereum'),
                ('0x11f0b5cca01b0f0a9fe6265ad6e8ee3419c68440', 'Paraswap (opt)', '80 PSP/20 WETH', 'optimism'),
                ('0xd20f6f1d8a675cdca155cb07b5dc9042c467153f', 'Byte Masons', '80 bOATH/20 WETH', 'optimism'),
                ('0xae8f935830f6b418804836eacb0243447b6d977c', 'Aavegotchi', '80 GHST/20 USDC', 'polygon'),
                ('0xb204bf10bc3a5435017d3db247f56da601dfe08a', 'THX', '80 THX/20 USDC', 'polygon'),
                ('0xe2f706ef1f7240b803aae877c9c762644bb808d8', 'Tetu', '80 TETU/20 USDC', 'polygon')
                ) AS t (address, project, label, blockchain)
    )
    
    , tvl AS (
        SELECT 
            l.pool_id
            , BYTEARRAY_SUBSTRING(l.pool_id, 1, 20) AS pool_address
            , l.blockchain
            , SUM(l.pool_liquidity_usd) AS tvl
        FROM balancer.liquidity l
        WHERE l.pool_liquidity_usd > 1 
            AND l.day = (CURRENT_DATE - interval '1' day)
        GROUP BY 1,2,3
    )
    
    , swaps AS (
        SELECT 
            t.project_contract_address
            , t.blockchain
            , SUM(t.amount_usd) FILTER(WHERE t.block_time >= now() - INTERVAL '24' hour) AS volume_24h
            , SUM(t.amount_usd) FILTER(WHERE t.block_time >= now() - INTERVAL '30' day) AS volume_30d
        FROM balancer.trades t
        GROUP BY 1,2
    )
    
    , fees_collected AS (
        SELECT 
            pool_address
            , blockchain
            , SUM(protocol_fee_collected_usd) FILTER(WHERE day >= now() - INTERVAL '24' hour) AS fees_collected_24h
            , SUM(protocol_fee_collected_usd) FILTER(WHERE day >= now() - INTERVAL '30' day) AS fees_collected_30d
            , SUM(protocol_fee_collected_usd) as fees_collected_all_time
        FROM balancer.protocol_fee t
        GROUP BY 1,2
    )    

SELECT 
    w.project
    , w.label
    , w.blockchain || 
        CASE 
            WHEN w.blockchain = 'arbitrum' THEN ' 🟦'
            WHEN w.blockchain = 'avalanche_c' THEN ' ⬜ '
            WHEN w.blockchain = 'base' THEN ' 🟨'
            WHEN w.blockchain = 'ethereum' THEN ' Ξ'
            WHEN w.blockchain = 'gnosis' THEN ' 🟩'
            WHEN w.blockchain = 'optimism' THEN ' 🔴'
            WHEN w.blockchain = 'polygon' THEN ' 🟪'
        END 
    AS symbol_blockchain
    , w.blockchain
    , t.pool_id
    , SUM(t.tvl) AS tvl
    , SUM(s.volume_24h) AS volume_24h
    , SUM(s.volume_30d) AS volume_30d
    , SUM(fees_collected_30d) AS fees_collected_30d
    , SUM(fees_collected_all_time) AS fees_collected_all_time
    , COALESCE(il.impermanent_loss, 0) as impermanent_loss
    , CONCAT(
        '<a target="_blank" href="https://dune.com/balancer/8020-initiative?Blockchain_t18ee8='
        , w.blockchain
        , '&Pool+Address_t95afa=', w.address, '">View Stats</a>'
    ) AS stats
    , w.address
FROM ve8020_pools w
LEFT JOIN tvl t 
    ON CAST(t.pool_address AS VARCHAR) = w.address 
    AND t.blockchain = w.blockchain 
LEFT JOIN swaps s 
    ON CAST(s.project_contract_address AS VARCHAR) = w.address 
    AND s.blockchain = w.blockchain
LEFT JOIN fees_collected r
    ON CAST(r.pool_address AS VARCHAR) = w.address
    AND r.blockchain = w.blockchain
LEFT JOIN dune.balancer.result_ve_8020_pools_impermanent_loss il
    ON CAST(il.pool_id as VARCHAR) = w.address
    AND il.blockchain = w.blockchain
GROUP BY 1,2,3,4,5,11,12,13
ORDER BY 6 DESC