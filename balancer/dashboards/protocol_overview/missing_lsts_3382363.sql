-- part of a query repo
-- query name: Missing LSTs
-- query link: https://dune.com/queries/3382363


SELECT * FROM (values
(0x6cda1d3d092811b2d48f7476adb59a6239ca9b95, 'rETH', 'arbitrum', 0x82af49447d8a07e3bd95bd0d56f35241523fbab1),
(0x190b2aa820495c0e92840e8fa699741976cd6439, 'rETH', 'base', 0x4200000000000000000000000000000000000006))
    as t (address, name, blockchain, equivalent)