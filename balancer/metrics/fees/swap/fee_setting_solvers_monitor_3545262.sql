-- part of a query repo
-- query name: Fee Setting Solvers Monitor
-- query link: https://dune.com/queries/3545262


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
        0x5866b21a5d43f068cf692c86cb8cf704b49b22a66822947dfc1959d690d9bc11,
        0x5d502be2a5480272b2fa64aa8b48ea56c4de1b2e1a45745f73e31d2d03b4e560,
        0xe436d32bc79647dfce92a5f9c2a41d2d83632599e7ea6976594ecaf0daf01a21,
        0x9dc032e1a01f928f936253fdfc7c082b1517d62bd4f9ec04091bb34e4b4c78d7,
        0x077a1f4bbdd1d10587828b09abaa8305afa6b3fef87e9405c6732ce528445668,
        0x6a024a59c6d7856aae802d69a23a10f9862a7e78d74b87e0d7f275be2c03f9cf,
        0xa7d6224a1ea13fde8c550330d706ed363ed46dc3c8a388fbcce803f4b5d16d3e,
        0x8a7ca73708b36d74cb41cad1ba32a06462b5d57edc08ba2fd9a0da3e2a8cbd2d,
        0xffd14a3862df3979536dfdb6cec57a375cf4e393c8b016bf1b09e4e5d697c49c,
        0x7568bc997740d7e2645cffd3b050975a94941e999746bfc262c9f4da92f5137d,
        0x2843412c025cd587aef836ccc949c53d021730295e79861d91b2cdd5be32a964,
        0x7f91e0859c61a629438b2eea33feccdc7a6d8c530e43272e369e2eaac11af8fc,
        0x0257286ef16d00b9354d87c66949db33f10938c2377a5662de2af0f4740194d5,
        0xdd4f0580eccc612ff235ef43c353af39083ea88fa0e53de873a57b02b8032fef,
        0x254c48e0f0abe4d217f089abe3e4827a37fd684dd2cec6a09d3d24854cd8ba04,
        0x8b352bac4f93bad9d7c29cfce2815129a7a270b2d09de41d1dd839d283e42644,
        0x8938f3a8990877ce9173f1c5f3eea2a5f866492ed37c9c0a8b3105b117b033cd,
        0xa630aecacd229a5d0c28f1c3afd3af6905153dbf3296bb17afb4442adcd36c00,
        0xbeeee78592a4480835c926b0322029e9ee5b8dece04e8e8af66821572f0af4ac,
        0xdb0a9b90338c21b4e31ec715443b3a7ebd8c62dc67d52d83357bee17186c9b77,
        0x32e2d5da1f42b57b2419bc798d3e53a6a49edacb6e941f6984c8767a17d125fe,
        0xb7101c89bc3f8bcd1d29f1c9ac60f81d921b73db9a2fe6be067e5ff3ad77fba2,
        0x35ee6fb4e41ad0b54c7eda5396ae233306d76422bb8dd174ebb09f19d312fb62,
        0xe2b38b994a342d3ee3eae3b1e519ce4ab9aab31faae5f1356fce46a091e92faf,
        0x828cfd0361f40dc0939697b029aebd8a728bc3b55f4ac8e24f5f1cc5b749bfb9,
        0x049f9bfc13a19866b1cc61d37b4816f328731e3098ce97d1bd7d184625c2027a,
        0x0136b11fbbbae2f3bdd184b84b85498be0a16049e7de767f044783ee240d9578,
        0x58f3795c82d4cc69e89cbf8bdc491cf36f708b01e9117afad20d39f22596a3a0,
        0x6e5deecbbd0aa77b2806ede20fc0e2d6890aa13a41bc5b0980448496d926a32a,
        0x08625510623f024c5aa61b52f737b333d0d96571c2307bbde2880cc950d4d7b4,
        0xcbf4a9c53f9980730136b842abb4eae67d12d92e877747de3b16e9b8af59ff41,
        0x77eddc9a99f278d6f9fdb7e8694f912d7291d2e87929fd2b1c920b4dbae84c1f,
        0x84560dcbf8667d9c53077f4001154f074a89dc89572ff9a826f3b6af63113d46,
        0x91d168400a7677272ede91340f1f35a0df4ed3b304f9d1ae325283738a480cb4,
        0xa7cb27ef4d13a8714f0886e3bf306793077c7c7ee889108d38e890f4d29756c6,
        0xed2929406469d86c66250a44ece38d7862e31c27c07e54c1b2cd5697154e31c5,
        0x0c52e1c406f7d2388ef219186aa0c1d851fa4a7f65f5bc0c7a0f62462cc43f0a,
        0x8594625d923d0108e882e965113154c4fac804a69fca735ee978d8e6fb6be8aa,
        0x57450913dbfdf0cd7e76b8eda0604c6c3557894b79da22bc1ef63d417b0309ab,
        0xe4b52ea8099ba237164a68d088b6a12ce5903c5b175aeea67c12874b7bb4d816,
        0xb83cbbc001655d27feb706f27d842caad09ee700388b8540080cdbaf970207a8,
        0x169f0c1a684741bcfb1bc461b584f7b16421a08a6ce30065c96ef723c8191ef2,
        0x4bec1d094d6940ffc92303695ff3788f4744c6813895aea6003bab099232e548,
        0xa2ee0cf79f285dae94dec770927c652c6a7da422c5e91b35cc66ed3fe78ba4e2,
        0x5278b397a85a7e02188b1db26f9a888526e83f7056ca3ed9156878c87696ed36,
        0x6fb05d86b8ee4035d58506984f04226b0d2a4b599f826499f4965a894fc82b87,
        0xc0fcac475f4a1ca1f292539439ac5f3a0f4aa3abb5796236c23099f4efb28cce,
        0x4e0b7e2f1f1a5bd1831793298fff302f221743fa259e4b77cf6bbb102170fc7f,
        0xd70fc3825c661692a3b4f70f9cc75abbe5437d2d59c9489856654dde59474e5a,
        0x83c29e66d39cc023a2002e1a4a3894a065e4397ad1c7bfc692ae4e800ad56347,
        0x3611e8b47a98965ca0b8fc607dea6ef4487f5538ab55e9d206b07c297550039a,
        0xcb590660b1beed73be07614c039a35b818403b2c36c104fd766c88846d6d8f4d,
        0x26eb7e4897756b4f940ef4d3c1f3d56eace06edb4320a49db330400cff2f7058,
        0xb7403045488184721a2a166e92be6c52fffd9b26e22c58150cc3f99e137e9939,
        0x3a246f9be6cc1ecd9e1ecd446938f3cc975ef2b2a20f8d8d2533182a700f6ea3,
        0xcb38a47934d9ebd3eefdcb55364c0f3b81a8e3141341f11587e9257b683b4d98,
        0x7cfa0b4c03834ba16ad987c7fe4984ccfa5b52c8ad2fe41c6088e8f2ea192d63,
        0x75d15d074d88588cf57297656b7ab8270075fff7797219369c065c5e136b8a12,
        0xd123bf78903328ff027c4528fda91b484942beea0aebfbba020d0638264d24ab,
        0x2fbf089c6e9383db683fd88e74e2256c523b6758c938a2a8093d80cf9d3e161c

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
    order by block_time desc, contract_address, block_number, index
)

select * from classification
where 1=1
and block_time > now() - interval '7' day
and check != 'OK'

UNION ALL

SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
order by block_number DESC, index ASC