-- part of a query repo
-- query name: fluid_blockchains
-- query link: https://dune.com/queries/5044493


with

-- liquidity contract deploy date
blockchains(blockchain, deploy_date) as (
    values
        ('ethereum', timestamp '2024-02-16')
      , ('arbitrum', timestamp '2024-06-10')
      , ('base', timestamp '2024-07-21')
      , ('polygon', timestamp '2025-05-01')
      , ('plasma', timestamp '2025-09-11')
      , ('bnb', timestamp '2025-12-15')
)

select *
from blockchains