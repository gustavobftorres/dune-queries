-- part of a query repo
-- query name: flashswaps
-- query link: https://dune.com/queries/889717


with foo as (
    select 
        explode(output_assetDeltas) as delta,
        call_tx_hash as tx_hash
    from  balancer_v2_ethereum.Vault_call_batchSwap b
    where call_success
),
flashswaps as (
    select tx_hash from (
        Select max(delta) as vMax, tx_hash
        From foo 
        Group by 2
    ) t
    where vMax <= 0
),
proceeds as (
    select call_tx_hash as tx_hash, 
    call_block_time as time, 
    token_address,
    -output_assetDeltas[id] as proceeds
    from balancer_v2_ethereum.Vault_call_batchSwap s
    lateral view posexplode(assets) as id, token_address
    where call_tx_hash in (select * from flashswaps)
),
proceeds_price as (
    select s.*, p.price, decimals
    from proceeds s
    inner join prices.usd p
    on date_trunc('minute',s.time) = p.minute
    and s.token_address = p.contract_address
    inner join tokens_ethereum.erc20 t
    on t.contract_address = s.token_address
),
revenue as (
    select tx_hash, time,
    sum(price * proceeds / power(10, decimals)) as revenue
    from proceeds_price
    group by 1, 2
),
revenue_cost as (
    select t.`from` as from_address, tx_hash, time, revenue, p.price * t.gas_used * t.gas_price / power(10, 18) as cost
    from revenue r
    inner join ethereum.transactions t
    on t.hash = r.tx_hash
    and t.block_time >= (select min(time) from revenue)
    inner join prices.usd p
    on p.minute = date_trunc('minute', r.time)
    and p.symbol = 'ETH'
)

select from_address, tx_hash, time, 
sum(revenue - cost) as profit 
from revenue_cost
group by 1,2,3
order by 3
