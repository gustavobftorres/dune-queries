-- part of a query repo
-- query name: tokens_native
-- query link: https://dune.com/queries/5825281


with

tokens_native(chain, symbol, price_symbol, price_address, decimals) as (
    -- values
    --       ('ethereum', 'ETH', 'WETH', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18)
    --     , ('arbitrum', 'ETH', 'WETH', 0x82af49447d8a07e3bd95bd0d56f35241523fbab1, 18)
    --     , ('base', 'ETH', 'WETH', 0x4200000000000000000000000000000000000006, 18)
    --     , ('polygon', 'POL', 'WPOL', 0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270, 18)
    --     , ('plasma', 'XPL', 'WXPL', 0x6100E367285b01F48D07953803A2d8dCA5D19873, 18)
    select chain, symbol, price_symbol, price_address, decimals
    from tokens.native
)

select *
from tokens_native
