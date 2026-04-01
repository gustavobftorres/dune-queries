-- part of a query repo
-- query name: balancer_contracts_check
-- query link: https://dune.com/queries/2767078


--https://colab.research.google.com/drive/1N5X8mmjmjzwhBftnmdkW7bCG2LHbW1iL

WITH 
    -- submitted_contracts
    sc AS (
        SELECT 'ethereum' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM ethereum.contracts_submitted WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'arbitrum' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM arbitrum.contracts_submitted WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'avalanche' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM avalanche_c.contracts_submitted WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'gnosis' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM gnosis.contracts_submitted WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'optimism' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM optimism.contracts_submitted WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'polygon' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM polygon.contracts_submitted WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'base' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM base.contracts_submitted WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'zkevm' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM zkevm.contracts_submitted WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
    ),
    -- decoded_contracts
    decoded AS (
        SELECT 'ethereum' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM ethereum.contracts WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'arbitrum' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM arbitrum.contracts WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'avalanche' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM avalanche_c.contracts WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'gnosis' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM gnosis.contracts WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'optimism' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM optimism.contracts WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'polygon' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM polygon.contracts WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'base' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM base.contracts WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
        UNION ALL
        SELECT 'zkevm' as blockchain, created_at, CAST("from" AS VARCHAR) AS "from", dynamic, factory, CAST(address AS VARCHAR) AS address, name, abi FROM zkevm.contracts WHERE namespace IN ('balancer_v3', 'balancer_v2', 'balancer')
    ),
    -- deployed contracts
    deployed AS (
        SELECT 
            deployment_task
            , cast(date_parse(split(deployment_task, '-')[1], '%Y%m%d') AS DATE) AS first_deployment_date
            , status
            , chain AS blockchain
            , name
            , CAST(address AS VARCHAR) AS address
            , formatted_abi AS abi
            , last_run_timestamp
        FROM (
            SELECT *, 
                CASE WHEN blockchain = 'mainnet' THEN 'ethereum' ELSE blockchain END AS chain, 
                array[replace(abi, U&'\0027', U&'\0022')] AS formatted_abi 
            FROM dune."balancer".dataset_balancer_deployments
        )
    ),


    submitted_with_mock AS (
        SELECT 
            blockchain as blockchain,
            created_at,
            "from",
            dynamic,
            factory,
            address,
            CONCAT('Mock',name) as name,
            abi 
        FROM sc
    ),
    decoded_with_mock AS (
        SELECT 
            blockchain as blockchain,
            created_at,
            "from",
            dynamic,
            factory,
            address,
            CONCAT('Mock',name)as name,
            abi 
        FROM decoded
    ),
    all_submitted AS (
        SELECT * FROM sc
        UNION ALL
        SELECT * FROM submitted_with_mock
    ),
    all_decoded AS (
        SELECT * FROM decoded
        UNION ALL
        SELECT * FROM decoded_with_mock
    ),

    deployed_not_submitted AS (
        SELECT blockchain, address FROM deployed EXCEPT SELECT blockchain, address FROM all_submitted
    ),
    deployed_not_decoded AS (
        SELECT blockchain, address FROM deployed EXCEPT SELECT blockchain, address FROM all_decoded
    ),
    cross_check AS (
        SELECT * FROM deployed_not_submitted EXCEPT SELECT * FROM deployed_not_decoded 
        UNION ALL 
        SELECT * FROM deployed_not_decoded EXCEPT SELECT * FROM deployed_not_submitted
    )

SELECT 
    dns.blockchain,
    d.name,
    dns.address,
    d.abi,
    d.last_run_timestamp, -- last time the Balancer Deployments Script was run
        CASE
            WHEN dns.blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://etherscan.io/address/0', SUBSTRING(CAST(dns.address AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN dns.blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://arbiscan.io/address/0', SUBSTRING(CAST(dns.address AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN dns.blockchain = 'polygon' THEN CONCAT('<a target "_blank" href="https://polygonscan.com/address/0', SUBSTRING(CAST(dns.address AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN dns.blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://gnosisscan.com/address/0', SUBSTRING(CAST(dns.address AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN dns.blockchain = 'optimism' THEN CONCAT('<a target="_blank" href="https://optimistic.etherscan.io/address/0', SUBSTRING(CAST(dns.address AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN dns.blockchain = 'avalanche_c' THEN CONCAT('<a target="_blank" href="https://snowtrace.io/address/0', SUBSTRING(CAST(dns.address AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN dns.blockchain = 'base' THEN CONCAT('<a target="_blank" href="https://basescan.org/address/0', SUBSTRING(CAST(dns.address AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN dns.blockchain = 'zkevm' THEN CONCAT('<a target "_blank" href="https://zkevm.polygonscan.com/address/0', SUBSTRING(CAST(dns.address AS VARCHAR), 2, 41), '">⛓</a>')
        END AS scan
FROM deployed_not_submitted dns 
LEFT JOIN deployed d ON dns.blockchain = d.blockchain 
    AND dns.address = d.address
WHERE d.status = 'ACTIVE' 
AND dns.blockchain != 'bsc'
AND dns.blockchain != 'mode'
AND dns.blockchain != 'fraxtal'
AND d.name NOT LIKE '%Library'
AND d.name NOT LIKE 'Mock%'
AND d.name NOT LIKE '%Lib'
AND d.name NOT LIKE '%Math'
AND d.name NOT LIKE '%Helper%'
AND d.name NOT LIKE '%Queries'
AND d.name NOT LIKE '%Adder'
AND d.name NOT LIKE '%Validation'
AND d.name NOT LIKE '%Relayer'
AND d.name NOT LIKE 'Authorizer%'
AND d.name NOT LIKE 'L2%'
AND d.name NOT LIKE 'Null%'
AND d.name NOT LIKE '%Proxy'
AND d.name NOT LIKE '%Registry'
AND d.name NOT LIKE 'Test%'
AND d.name NOT LIKE '%Delegation'
AND d.name NOT LIKE '%Checker'
AND d.name NOT LIKE '%Scheduler'
AND d.name NOT LIKE '%Remapper'
AND d.name NOT LIKE '%Adaptor'
AND d.name NOT LIKE 'BALTokenHolderFactory'
AND d.name NOT LIKE '%Provider'
AND d.name NOT LIKE 'VaultExtension' --ABI included on vault
AND d.name NOT LIKE 'VaultFactory' --Not needed for now
--AND d.name NOT LIKE '%Router%' --Not needed for now
AND dns.address != '0x6337949cbc4825bbd09242c811770f6f6fee9ffc' --already decoded via factory
AND dns.address != '0xfeb1a24c2752e53576133cdb718f25bc64ebdd52' --already decoded via factory
AND dns.address != '0x4132f7acc9db7a6cf7be2dd3a9dc8b30c7e6e6c8' --already decoded via factory
ORDER BY 1, 2;