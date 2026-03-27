-- part of a query repo
-- query name: all balancer reclamms
-- query link: https://dune.com/queries/5244620


SELECT
    chain,
    pool
FROM balancer_v3_multichain.vault_evt_poolregistered
WHERE 1=1
AND chain IN (
    'base',
    'arbitrum',
    'ethereum'
)
AND factory IN (
    0x355bd33f0033066bb3de396a6d069be57353ad95
)
