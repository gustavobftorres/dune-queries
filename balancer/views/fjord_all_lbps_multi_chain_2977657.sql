-- part of a query repo
-- query name: FJORD / ALL LBPs / Multi-chain
-- query link: https://dune.com/queries/2977657


-- three contracts deployed on polygon 
-- v1 0xeb3F67501dEfF337B45980135fc7789e761f706c
-- v2 0x22D15E202538e90d6fDaE5044A4D6a28453aA4C5
-- v6 0x1861e2A790143284Ec1d03B4fcB94f3C3761C00d

select  name,
        'Arbitrum' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        FROM fjord_arbitrum.FjordLbpProxyV6_evt_PoolCreated

UNION ALL
select  name,
        'Arbitrum' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        FROM fjord_arbitrum.CopperProxyV1_evt_PoolCreated
UNION ALL
select  name,
        'Arbitrum' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        FROM fjord_arbitrum.CopperProxyV2_evt_PoolCreated
UNION ALL 
-- two contracts deployed on BNB 
-- v3 0xE22926EA2642B9cD3301cbC4ea47b223f82D702d
-- v6 0x8B5c2390C3A2a39189Af4D7B9b24b69B1071991b
select  name,
        'BSC' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        FROM fjord_bnb.CopperProxy_v3_evt_PoolCreated
UNION ALL
select  name,
        'BSC' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        FROM fjord_bnb.FjordLbpProxyV6_evt_PoolCreated
UNION ALL
-- two contracts deployed on AVALANCHE
-- v3 0x992845aDF2f97Be4A428884b9345c19D5E25725d
-- v6 0x2D8575adC1112f7A3a92051f4DB1B9ca2D242F47

select  name,
        'Avalanche' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        FROM fjord_avalanche_c.CopperProxyV3_evt_PoolCreated
UNION ALL 
select  name,
        'Avalanche' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        FROM fjord_avalanche_c.FjordLbpProxyV6_evt_PoolCreated
UNION ALL
-- three contracts deployed on polygon 
-- v1 0x9e7d0197693a5282cdb2a1cfd3081d261e3e04a3
-- v2 0x7388feb5a04990bb4c7570e68f1b37ab06c1aafd
-- v6 0xa8cfdccedb1c6e97b46bfc2fa0f086e0438e58d8

select  name,
        'Polygon' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        FROM fjord_polygon.FjordLbpProxyV6_evt_PoolCreated
UNION ALL
select  name,
        'Polygon' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        -- v1 contrcact on polygon
        FROM alchemist_polygon.CopperProxy_evt_PoolCreated
UNION ALL
select  name,
        'Polygon' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        FROM fjord_polygon.CopperProxyV2_evt_PoolCreated
UNION ALL 
-- two contracts deployed on optimism
-- v5 0xbb27ED373d5A86290D8734851bb7a5C698B0267c
-- v6 0x5897aA557176dA79FE78a168EA9cC945CF50A541

select  name,
        'Optimism' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        FROM fjord_optimism.FjordLbpProxyV6_evt_PoolCreated
UNION ALL
select  name,
        'Optimism' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        FROM fjord_optimism.FjordLbpProxyV5_evt_PoolCreated
UNION ALL
select  name,
        'Ethereum' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights from balancer_v2_ethereum.CopperProxy_evt_PoolCreated
UNION ALL
select  name,
         'Ethereum' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights FROM balancer_v2_ethereum.CopperProxyV2_evt_PoolCreated
UNION ALL 
select  name,
         'Ethereum' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,SUBSTRING(CAST(poolId AS VARCHAR), 1, 42) AS pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        FROM fjord_ethereum.FjordLbpProxyV6_evt_PoolCreated
UNION ALL
select  name,
         'Ethereum' as chain
        ,contract_address
        ,evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,owner
        ,pool as pool_address
        ,poolId
        ,swapEnabledOnStart
        ,swapFeePercentage/1e18 as swapFeePercentage
        ,symbol
        ,tokens
        ,weights
        FROM query_2972525 
            WHERE pool IN (
                      '0x327c7cb1ad72216ac90988fae4960bc2d5ef9738',
                      '0x13c7d3b51c304add517c40a39d8a85b0cdea605f',
                      '0xf2b7794b89ea4fd2abfe66dcb6529a27c03d429e',
                      '0x89d4a55ca51192109bb85083ff7d9a13ab24c8a1',
                      '0x6d68d7b0ca469bd1171f81a895e649d86d523c20',
                      '0xc79b9b75cd0e9567a000eeb8f6e46b3d074ac38c',
                      '0xa30ac4a3bf3f680a29eb02238280c75acbb89d6d',
                      '0xb61bef0cf17b25542c061ed861f270d5ac88a6b7',
                      '0xd153e1de63b478213b7b62bf47dcc4099608b1ae',
                      '0x9c4626fcacc114be8134e655cd6a93d2987863dd',
                      '0x4ddf308520864ecfc759c49e72acfc96c023ed90',
                      '0x68184be6773816f753192cece797ef67841ff1a7',
                      '0x656201f721012dda23ab7544021d79ca28b756d9',
                      '0x3cbbbe0d1a6284f82e88a06b55641ee1146cd540',
                      '0x6a06e2a5cb13fcd54b28f3f90144e64952a3a0b4',
                      '0x4eebc19e5f29dec3dea07f66b9e707afc8f28c06',
                      '0x6aa8a7b23f7b3875a966ddcc83d5b675cc9af54b',
                      '0x064dd1c629731e31a944aa434acde33f3b7eae99',
                      '0x9234f3a62154f7fcda5346c35d8d53f608651864'
                    )
UNION ALL

SELECT 
    'Vesta Finance' AS name, 
    'Arbitrum' AS chain, 
    0x0000000000000000000000000000000000000000 AS contract_address,
    0x0000000000000000000000000000000000000000000000000000000000000000 AS evt_tx_hash,
     TIMESTAMP '2022-01-31 21:00' AS evt_block_time,
    000000 AS evt_block_number,
    0x82b864ccda46fd6bd851990b951417969da7b3dd AS owner, 
    '0x289e1626d76678177E55489AE3d765690fbeaa03' AS pool_address,
    0x289e1626d76678177e55489ae3d765690fbeaa030002000000000000000005b3 AS poolId,
    false AS swapEnabledOnStart,
    0.025 AS swapFeePercentage,
    'VSTA_LBP' AS symbol,
    ARRAY[0xaf88d065e77c8cC2239327C5EDb3A432268e5831, 0xa684cd057951541187f288294a1e1c2646aa2d24] AS tokens,
    transform(ARRAY[960000000000000000, 40000000000000000] , x -> CAST(x AS uint256)) AS weights


            