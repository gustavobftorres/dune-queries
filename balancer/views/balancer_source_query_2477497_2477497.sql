-- part of a query repo
-- query name: Balancer_Source_query_2477497
-- query link: https://dune.com/queries/2477497


WITH balancer_source as (
SELECT  '0x0000006daea1723962647b7e189d311d757fb793' as address, 'wintermute' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x9799b475dec92bd99bbdd943013325c36157f383' as address, 'bancor' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x9008d19f58aabd9ed0d60971565aa8510560ab41' as address, 'gnosis_v2' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x111111125434b319222cdbf8c261674adb56f3ae' as address, '1inch' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT 'xdcdbf71a870cc60c6f9b621e28a7d3ffd6dd4965' as address, 'lido_relayer' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0xdef171fe48cf0115b1d80b88dc8eab59176fee57' as address, 'paraswap' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x1bd435f3c054b6e901b7b108a0ab7617c808677b' as address, 'paraswap' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x26d26b1a0243566d1cd38ff9afd5fd3f0fb6cbb4' as address, 'open ocean' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0xba12222222228d8ba445958a75a0704d566bf2c8' as address, 'vault' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x1111111254fb6c44bac0bed2854e76f90643097d' as address, '1inch' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x3e66b66fd1d0b02fda6c811da9e0547970db2f21' as address, 'exchange_proxy' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x6317c5e82a06e1d8bf200d21f4510ac2c038ac81' as address, 'exchange_proxy' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x11111254369792b2ca5d084ab5eea397ca8fa48b' as address, '1inch' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0xdef1c0ded9bec7f1a1670819833240f027b25eff' as address, 'matcha' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x11111112542d85b3ef69ae05771c2dccff4faa26' as address, '1inch' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x881d40237659c251811cec9c364ef91dc08d300c' as address, 'metamask' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x3328f5f2cecaf00a2443082b657cedeaf70bfaef' as address, 'gnosis_v2' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x1b58901493b7a368a3d13db585f0da71a198fcc3' as address, 'arbitrage bot' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x1b58901493b7a368a3d13db585f0da71a198fcc3' as address, 'linear arb bot' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0x1111111254eeb25477b68fb85ed929f73a960582' as address, '1inch' as name, 'balancer_source' as "type", 'balancerlabs' as author
UNION ALL
SELECT '0xad3b67BCA8935Cb510C8D18bD45F0b94F54A968f' as address, '1inch' as name, 'balancer_source' as "type", 'balancerlabs' as author
)

SELECT * FROM balancer_source
