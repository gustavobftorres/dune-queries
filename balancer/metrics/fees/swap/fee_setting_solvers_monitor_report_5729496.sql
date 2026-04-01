-- part of a query repo
-- query name: Fee Setting Solvers Monitor (REPORT)
-- query link: https://dune.com/queries/5729496


with 
fee_changes as (
    select a.* from balancer.pools_fees a
),
side_by_side_fee_changes as (
    select blockchain, contract_address, block_number, block_time, index,
           lag(swap_fee_percentage) over (partition by blockchain, contract_address order by ARRAY [block_number] || index)/1e18 as prev_fee,
           swap_fee_percentage/1e18 as fee,
           lead(swap_fee_percentage) over (partition by blockchain, contract_address order by ARRAY [block_number] || index)/1e18 as next_fee,
           lag(tx_hash) over (partition by blockchain, contract_address order by ARRAY [block_number] || index) as prev_fee_tx_hash,
           tx_hash as tx_hash,
           lead(tx_hash) over (partition by blockchain, contract_address order by ARRAY [block_number] || index) as next_fee_tx_hash
    from fee_changes
),
classification as (
    select 
        CASE 
        WHEN tx_hash = 0x9f841a19b870502f3848e22dad2a179bd9cc771003497315af8e72c0c357b889
        AND blockchain = 'arbitrum'
            THEN 'OK'
        WHEN tx_hash = 0xa9db2fcec64bab464e0c4df6325adfc096bc71ad191fa0c18a99536a9f1ed14e
        AND blockchain = 'optimism'
            THEN 'OK' --false positive
        WHEN tx_hash IN (
        0x85524b6e3dd8ddb445f683a8475e910c78dc5298fc73695a3f3d1ec861889dbc,
        0x89023569c93d75ba7df4b936582fe997dc3a30ff911341b4af0c0b829f490ab7,
        0x8f245a8ff37a1ee543836831de2fc29e96ba907a9c96317d1a44d69e6a12b8d4,
        0x41921fa297cf42577536f456b5736bd10cf40d27275e397e3f55fc2f07109603,
        0xcbbe9eddfa31559362457811d20500bcd40dfe588b28a44e5fdaa8a089f5ff86,
        0xdc8db6d56c7a6a374e0cff26615e5064a5803fa0c0c125477da37960df3f1982,
        0x085274de14270d32659781746d90c60f01da4804800093d38c6453022b919105,
        0x87fbc376c877cd6efe20744d61a936e5ba4d40f2de1c9f57238ba72d10d0756f,
        0x2ba82ce12dac7b3fa5809339d8809012a7f728eeb72299bba1e6d3aff13b43db,
        0x2ba82ce12dac7b3fa5809339d8809012a7f728eeb72299bba1e6d3aff13b43db,
        0xe10220e11e4926073ad4a907407cf3eb684a1f0e2c60814d672dd0cd44efbcf0,
        0x51a074f991db9d55a6c0b81aed999f9ec2d5040da5dcd2a882cfdd2d89436a66,
        0x5141ab1b7c825069d1878a05eefeeecee78f044e3bf04ce91cc555e132b2defe,
        0x9299cef7eaa99951b5b236bc9f848e39b96a0b51e63ca9d2df8a09f28c01a65a,
        0x25b599b842d1fcbe8b3a1ea7669e7b96540851b3e5e0c8f978394ea72acca8ad,
        0x6bb1b75d3449b6cb023281509cc22a63629d012c52141071d982303f1284cd6b,
        0x049ef5a043ad8fc47d87a2d87090c64a41f63866caa645bc9fadca0f5a781df9,
        0x6b4161e69f55b5a8c9075d4a3c8d38916d5d00f24cd6ea078bad2aeb0b3db2fe,
        0xd2f128c1524df97d677da2ea1f45b83a7216a1edf841233bc26217167f5622a7,
        0x8f8b97f3cd9dd69489f6b80a571ec97089da31bece4463f7f1d8bd6081088094,
        0x773cb91ac9f6f386d1962bedfb0c0fbc8098dfd9444982a966975da2bd74af9b,
        0xb0566e5018499d67a2c995c911325a439722ab1b1dd222bba9b8a6b426af9398,
        0x46bb2c7012f5c4fa3b92907dba91807e6a0cd49e340db7348bcf9429f09a4089,
        0x5ce3f7addda24c47308ddcd6ca1125451ae18635b90e7b1fb29a109fd86a5180,
        0xab5f9f8c9f4056bade060d4d12575c15e3e260bb806596db041a2d876d6e2959,
        0xf405867a86ff2c7e418974f2d7e16d9a42aca07c48907028fbf339969230748c,
        0x41c0f60287e751e5cb6304004c461a238eba391b942514593392cf0f69582551,
        0x431058b9a7683526d7e34bd58492d339b9c5e0068019502e5466a3008bf57e48,
        0x730f80c55b0320ec8b01293acae58bb5f1f7bc8fd1a1f6b5a0a7b1a59d9deeae,
        0xe811f3f39a4b6f73a0b9c322cf459a45a5281e4df946c4700a84d55402b0b154,
        0x425cd579a0173f308ed37591f0ef92cdd6e1ac7d18855b804d2c903dc85ba673,
        0x5d502be2a5480272b2fa64aa8b48ea56c4de1b2e1a45745f73e31d2d03b4e560,
        0x5866b21a5d43f068cf692c86cb8cf704b49b22a66822947dfc1959d690d9bc11,
        0xe436d32bc79647dfce92a5f9c2a41d2d83632599e7ea6976594ecaf0daf01a21,
        0x9dc032e1a01f928f936253fdfc7c082b1517d62bd4f9ec04091bb34e4b4c78d7,
        0x077a1f4bbdd1d10587828b09abaa8305afa6b3fef87e9405c6732ce528445668,
        0x6a024a59c6d7856aae802d69a23a10f9862a7e78d74b87e0d7f275be2c03f9cf,
        0xa7d6224a1ea13fde8c550330d706ed363ed46dc3c8a388fbcce803f4b5d16d3e
        )
        AND blockchain = 'ethereum'
            THEN 'OK' --false positives
        WHEN tx_hash IN (
            0x87ec5066bb28ea9597688d47ed88ce19706f4f105faf5738143d6bba9db76429, -- RAVEN BUG
            0x88c3bd0f2ccc114a129f502ca9f545c6d676c20252132bf2b452c5d7924dfb71, -- RAVEN BUG
            0x8c2472e854859c62b0a69214463d3141b4f9dd2ab33644d12104ef2b119dbb9d, -- RAVEN BUG
            0x9acb5a5064b2bb1b17e5ae4042cbe918ddff2716843a8b5d8f23994c0be3bc82 -- RAVEN BUG
            ) THEN 'RAVEN BUG'
        WHEN tx_hash IN (
            0xb6e86664c970ba7e9801a39514146d4b8a8b8676fda6f473dce246e48a0ae7c7 -- BARTER BUG
        ) THEN 'BARTER BUG'
        WHEN tx_hash = next_fee_tx_hash and prev_fee <> next_fee
            THEN 'BAD SOLVER'
        ELSE 'OK'
        END AS check,
        *
    from side_by_side_fee_changes
    where prev_fee is not null
),
bad_fees_ranked as (
    select 
        *,
        ROW_NUMBER() OVER (PARTITION BY blockchain, contract_address ORDER BY block_time, block_number, index) as rn
    from classification
    where check != 'OK'
    and block_time > now() - interval '30' day
)
select 
    check,
    blockchain,
    contract_address,
    prev_fee,
    fee,
    next_fee,
    tx_hash
from bad_fees_ranked
where rn = 1
order by block_time desc
