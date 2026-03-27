-- part of a query repo
-- query name: micro add/remove on v2
-- query link: https://dune.com/queries/3928469


with all_add_removes as (
    select 'ethereum' as blockchain, b.pooLId, b.evt_block_time, b.evt_tx_hash, t.token, t.delta
    from balancer_v2_ethereum.Vault_evt_PoolBalanceChanged b
    cross join unnest(tokens, deltas) as t(token, delta)
    union all
    select 'arbitrum' as blockchain, b.pooLId, b.evt_block_time, b.evt_tx_hash, t.token, t.delta
    from balancer_v2_arbitrum.Vault_evt_PoolBalanceChanged b
    cross join unnest(tokens, deltas) as t(token, delta)
    union all
    select 'optimism' as blockchain, b.pooLId, b.evt_block_time, b.evt_tx_hash, t.token, t.delta
    from balancer_v2_optimism.Vault_evt_PoolBalanceChanged b
    cross join unnest(tokens, deltas) as t(token, delta)
),

add_removes_scaled18 as (
    select a.*, symbol, delta * power(10, 18 - decimals) as delta_scaled18
    from all_add_removes a
    join prices.usd p
    on p.contract_address = a.token
    and p.blockchain = a.blockchain
    and p.minute = current_date
)

select blockchain, evt_block_time, symbol, delta_scaled18, poolId, evt_tx_hash
from add_removes_scaled18
where (delta_scaled18 < 1e6 and delta_scaled18 > 0)
or (delta_scaled18 > -1e6 and delta_scaled18 < 0)
