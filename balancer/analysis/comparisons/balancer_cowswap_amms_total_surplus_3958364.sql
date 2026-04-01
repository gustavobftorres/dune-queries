-- part of a query repo
-- query name: Balancer CoWSwap AMMs total surplus
-- query link: https://dune.com/queries/3958364


with
    t1 as (
        select
            case
                when token_1_transfer_usd > 0 then token_1_transfer_usd + token_1_balance_usd * token_2_transfer_usd / (token_2_balance_usd - token_2_transfer_usd)
                else token_2_transfer_usd + token_2_balance_usd * token_1_transfer_usd / (token_1_balance_usd - token_1_transfer_usd)
            end as surplus,
            protocol_fee_usd
        from
            dune.balancer.result_b_cow_amm_base_table
        where
            istrade
    )
select
    SUM(surplus)
from
    t1
where
    surplus > 0