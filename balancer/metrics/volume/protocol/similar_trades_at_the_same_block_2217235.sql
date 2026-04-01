-- part of a query repo
-- query name: similar trades at the same block
-- query link: https://dune.com/queries/2217235


select *
from balancer_v2_polygon.Vault_evt_Swap
where poolId = 0xa3283e3470d3cd1f18c074e3f2d3965f6d62fff200010000000000000000011b
and tokenIn = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- USDC
and tokenOut = 0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6 -- WBTC
and evt_block_number = 40320684
