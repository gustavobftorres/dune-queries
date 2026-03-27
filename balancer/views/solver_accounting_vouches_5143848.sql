-- part of a query repo
-- query name: Solver Accounting: Vouches
-- query link: https://dune.com/queries/5143848


with polygon_vouching as (
    select
        'polygon' as chain, --noqa: RF04
        contract_address,
        tx_hash as evt_tx_hash,
        tx_from as evt_tx_from,
        tx_to as evt_tx_to,
        null as evt_tx_index,
        index as evt_index,
        block_time as evt_block_time,
        block_number as evt_block_number,
        block_date as evt_block_date,
        from_hex(substr(cast(topic2 as varchar), 27, 40)) as bondingPool, --noqa: CP02
        from_hex(substr(cast(data as varchar), 27, 40)) as cowRewardTarget, --noqa: CP02
        from_hex(substr(cast(topic3 as varchar), 27, 40)) as sender,
        from_hex(substr(cast(topic1 as varchar), 27, 40)) as solver
    from polygon.logs
    where contract_address = 0xaaa4de096d02ae21729aa31d967e148d4e3ae501 and topic0 = 0xd30c692ff1e6e1e96d8aca701b7f8118d58f64ce4c680feda75c0fc76524f7fa
),

bnb_vouching as (
    select
        'bnb' as chain, --noqa: RF04
        contract_address,
        tx_hash as evt_tx_hash,
        tx_from as evt_tx_from,
        tx_to as evt_tx_to,
        null as evt_tx_index,
        index as evt_index,
        block_time as evt_block_time,
        block_number as evt_block_number,
        block_date as evt_block_date,
        from_hex(substr(cast(topic2 as varchar), 27, 40)) as bondingPool, --noqa: CP02
        from_hex(substr(cast(data as varchar), 27, 40)) as cowRewardTarget, --noqa: CP02
        from_hex(substr(cast(topic3 as varchar), 27, 40)) as sender,
        from_hex(substr(cast(topic1 as varchar), 27, 40)) as solver
    from bnb.logs
    where contract_address = 0xaaa4de096d02ae21729aa31d967e148d4e3ae501 and topic0 = 0xd30c692ff1e6e1e96d8aca701b7f8118d58f64ce4c680feda75c0fc76524f7fa
),
linea_vouching as (
    select
        'linea' as chain, --noqa: RF04
        contract_address,
        tx_hash as evt_tx_hash,
        tx_from as evt_tx_from,
        tx_to as evt_tx_to,
        null as evt_tx_index,
        index as evt_index,
        block_time as evt_block_time,
        block_number as evt_block_number,
        block_date as evt_block_date,
        from_hex(substr(cast(topic2 as varchar), 27, 40)) as bondingPool, --noqa: CP02
        from_hex(substr(cast(data as varchar), 27, 40)) as cowRewardTarget, --noqa: CP02
        from_hex(substr(cast(topic3 as varchar), 27, 40)) as sender,
        from_hex(substr(cast(topic1 as varchar), 27, 40)) as solver
    from linea.logs
    where contract_address = 0xaaa4de096d02ae21729aa31d967e148d4e3ae501 and topic0 = 0xd30c692ff1e6e1e96d8aca701b7f8118d58f64ce4c680feda75c0fc76524f7fa
),
plasma_vouching as (
    select
        'plasma' as chain, --noqa: RF04
        contract_address,
        tx_hash as evt_tx_hash,
        tx_from as evt_tx_from,
        tx_to as evt_tx_to,
        null as evt_tx_index,
        index as evt_index,
        block_time as evt_block_time,
        block_number as evt_block_number,
        block_date as evt_block_date,
        from_hex(substr(cast(topic2 as varchar), 27, 40)) as bondingPool, --noqa: CP02
        from_hex(substr(cast(data as varchar), 27, 40)) as cowRewardTarget, --noqa: CP02
        from_hex(substr(cast(topic3 as varchar), 27, 40)) as sender,
        from_hex(substr(cast(topic1 as varchar), 27, 40)) as solver
    from plasma.logs
    where contract_address = 0xaaa4de096d02ae21729aa31d967e148d4e3ae501 and topic0 = 0xd30c692ff1e6e1e96d8aca701b7f8118d58f64ce4c680feda75c0fc76524f7fa
),
multichain_vouching as (
    select *
    from cow_protocol_multichain.vouchregister_evt_vouch
    union distinct
    select *
    from polygon_vouching
    union distinct
    select *
    from bnb_vouching
    union distinct
    select *
    from linea_vouching
    union distinct
    select *
    from plasma_vouching
)

select
    contract_address,
    evt_tx_hash,
    evt_tx_from,
    evt_tx_to,
    evt_tx_index,
    evt_index,
    evt_block_time,
    evt_block_number,
    evt_block_date,
    bondingPool, --noqa: CP02
    case
        when cowRewardTarget = 0x291445119993addc3c155e214e097e01a365067f then 0x9B47D63fd901FC03d0C1213431Ac6Ea0Bd3F9137
        when cowRewardTarget = 0xb98addcf799a8d7473020b41714c688af8107c3e then 0x9B47D63fd901FC03d0C1213431Ac6Ea0Bd3F9137
        when cowRewardTarget = 0xe381374a4d7571ad36b0d809f18c41375fc2ad36 then 0x9B47D63fd901FC03d0C1213431Ac6Ea0Bd3F9137
        when cowRewardTarget = 0x6abf06d96385b7ab2f2a7c745184244593401a67 then 0x9B47D63fd901FC03d0C1213431Ac6Ea0Bd3F9137
        when cowRewardTarget = 0xcf3cf8a607b91f4e4b668490c3e9cb104c430aee then 0x9B47D63fd901FC03d0C1213431Ac6Ea0Bd3F9137
        when cowRewardTarget = 0x6b3fc681c50828ce8a4790366a5cb4b6ab959f27 then 0x9B47D63fd901FC03d0C1213431Ac6Ea0Bd3F9137
        else cowRewardTarget 
    end as cowRewardTarget,
    sender,
    solver
from multichain_vouching
where chain = '{{blockchain}}'
