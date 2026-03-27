-- part of a query repo
-- query name: BGP labels
-- query link: https://dune.com/queries/197448


with trades as (
        select *
        from dex.trades
        where project = 'Balancer'
        and version = '2'
        and tx_to = '\x9008d19f58aabd9ed0d60971565aa8510560ab41'
    ),

    data as (
        select trades -> 0 ->> 'appData' AS app_id, t.*
        from gnosis_protocol_v2."GPv2Settlement_call_settle" g
        join trades t ON t.tx_hash = g.call_tx_hash
    )

select app_id, count(*), sum(usd_amount)
from data
group by 1
order by 2 desc

-- Balancer
-- 0x0000000000000000000000000000000000000000000000000000000000000001
-- 0x0000000000000000000000000000000000000000000000000000000000000002

-- BGP
-- 0xe9f29ae547955463ed535162aefee525d8d309571a2b18bc26086c8c35d781eb

-- CowSwap
-- 0x487b02c558d729abaf3ecf17881a4181e5bc2446429a0995142297e897b6eb37
-- 0xe4d1ab10f2c9ffe7bdd23c315b03f18cff90888d6b2bb5022bacd46ab9cddf24