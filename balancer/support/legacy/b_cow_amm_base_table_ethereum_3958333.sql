-- part of a query repo
-- query name: b_cow_amm_base_table_ethereum
-- query link: https://dune.com/queries/3958333


with
  ethereum_cow_amms_temp as (
    SELECT 
        a.contract_address AS cow_amm_address,
        a.token AS token_1_address,
        b.token AS token_2_address
    FROM b_cow_amm_ethereum.BCoWPool_call_bind a
    JOIN b_cow_amm_ethereum.BCoWPool_call_bind b
    ON a.contract_address = b.contract_address
    WHERE a.token < b.token
    AND a.call_success),
  
 ethereum_cow_amms as (
    select
      'ethereum' as  blockchain,
      a.cow_amm_address,
      a.token_1_address,
      b.symbol as token_1_symbol,
      a.token_2_address,
      c.symbol as token_2_symbol
    from
      ethereum_cow_amms_temp a
      join tokens.erc20 b on a.token_1_address = b.contract_address
      join tokens.erc20 c on a.token_2_address = c.contract_address
  ),

  cow_amms_temp as (
      select * from ethereum_cow_amms
  ),
  
  cow_amms as (
      select *, count(cow_amm_address) over (order by cow_amm_address, token_1_address, token_2_address) as
      cow_amm_nb from cow_amms_temp
  ),

--all the net transfers to/from the cow amms
transfers_temp as (
      SELECT 'ethereum' as blockchain, contract_address as token_address,
        (CASE WHEN "from" IN (SELECT cow_amm_address from ethereum_cow_amms_temp) then "from" else to end) as cow_amm_address,
        (CASE WHEN "from" IN (SELECT cow_amm_address from ethereum_cow_amms_temp) then -value else value end) as net_value,
        evt_block_time as time, evt_tx_hash as tx_hash
      FROM erc20_ethereum.evt_transfer
      WHERE ("from" IN (SELECT cow_amm_address from ethereum_cow_amms)
        or "to" IN (SELECT cow_amm_address from ethereum_cow_amms))
        AND evt_block_time > TIMESTAMP '2024-07-01'
        
  --add also the deposit events for WETH
  UNION
      SELECT 'ethereum' as blockchain, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token_address,
      varbinary_substring(topic1,13,20) as cow_amm_address, varbinary_to_uint256(data) as net_value,
      block_time as time, tx_hash
      from ethereum.logs
      where varbinary_substring(topic1,13,20) in (select cow_amm_address from ethereum_cow_amms)
      and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
      and topic0 = 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c
      AND block_time > TIMESTAMP '2024-07-01'  
  --add the withdrawals events
  UNION
      SELECT 'ethereum' as blockchain, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token_address,
      varbinary_substring(topic1,13,20) as cow_amm_address, -varbinary_to_uint256(data) as net_value,
      block_time as time, tx_hash
      from ethereum.logs
      where varbinary_substring(topic1,13,20) in (select cow_amm_address from ethereum_cow_amms)
      and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
      and topic0 = 0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65
      AND block_time > TIMESTAMP '2024-07-01'  
  ),
  
   transfers as(
        select blockchain, token_address, cow_amm_address, sum(net_value) as net_value, time, tx_hash
        from transfers_temp
        group by blockchain, token_address, cow_amm_address, time, tx_hash
    ),
  
  --beware of this filter if the cowamm was created multiple times (with different pairs)
    transfers_token_1 as(
        select t.blockchain, t.cow_amm_address, a.cow_amm_nb, time, tx_hash,token_address as token_1_address, token_2_address, net_value as token_1_transfer
        from transfers t
        join cow_amms a on t.cow_amm_address = a.cow_amm_address and t.blockchain = a.blockchain
        and token_address = token_1_address
    ),
    transfers_token_2 as(
        select t.blockchain, t.cow_amm_address, a.cow_amm_nb, time, tx_hash,token_address as token_2_address, token_1_address, net_value as token_2_transfer
        from transfers t
        join cow_amms a on t.cow_amm_address = a.cow_amm_address and t.blockchain = a.blockchain
        and token_address = token_2_address
    ),
    
     transfers_in_line as(
        select coalesce(t1.blockchain, t2.blockchain) as blockchain,
        coalesce(t1.cow_amm_address, t2.cow_amm_address) as cow_amm_address,
        coalesce(t1.time, t2.time) as time,
        coalesce(t1.tx_hash, t2.tx_hash) as tx_hash,
        coalesce(t1.token_1_address, t2.token_1_address) as token_1_address, 
        coalesce(t1.token_2_address, t2.token_2_address) as token_2_address, 
        coalesce(t1.token_1_transfer, 0) as token_1_transfer,
        coalesce(t2.token_2_transfer, 0) as token_2_transfer
        from cow_amms a
        join transfers_token_1 t1 on t1.cow_amm_nb = a.cow_amm_nb
        full outer join transfers_token_2 t2 on t2.cow_amm_nb = a.cow_amm_nb and t1.time = t2.time
        ),
  
  trades as(
        select 'ethereum' as blockchain, t.tx_hash, t.order_uid, t.trader as cow_amm_address,
        cast(r.data.protocol_fee as int256) as protocol_fee,
        from_hex(r.data.protocol_fee_token) as protocol_fee_token,
        least(buy_token_address, sell_token_address) as token_1_address, greatest(buy_token_address, sell_token_address) as token_2_address,
        t.block_time as time
        from cow_protocol_ethereum.trades t
        left join cowswap.raw_order_rewards r on cast(r.order_uid as varchar) = cast(t.order_uid as varchar)
        where trader in (select cow_amm_address from ethereum_cow_amms)
    ),
    
   cow_amms_evolution_temp as(
        select a.blockchain, a.cow_amm_address, a.cow_amm_nb, a.token_1_address, token_1_symbol, a.token_2_address, token_2_symbol, 
        l.token_1_transfer, l.token_2_transfer, l.time, l.tx_hash,
        -- for protocol_fee, the -1 is a trick used in the next table to show that no fees where applied, and therefore protocol_fee_token is null
        coalesce(t.protocol_fee, -1) as protocol_fee, t.protocol_fee_token
        from cow_amms a
        join transfers_in_line l on a.cow_amm_address = l.cow_amm_address and a.blockchain = l.blockchain
        and l.token_1_address = a.token_1_address and l.token_1_address = a.token_1_address
        left join trades t on a.cow_amm_address = t.cow_amm_address and l.tx_hash = t.tx_hash and l.blockchain = t.blockchain 
        and t.token_1_address = a.token_1_address and t.token_1_address = a.token_1_address
        
    ),
    
    cow_amms_evolution as (
        select t.blockchain, cow_amm_address, cow_amm_nb, time, tx_hash, 
        token_1_address, token_1_symbol, token_1_transfer, power(10, -coalesce(p1.decimals, p11.decimals))*token_1_transfer*coalesce(p1.price, p11.price) as token_1_transfer_usd,
        sum(token_1_transfer) over (partition by cow_amm_nb order by time ASC) as token_1_balance, 
        power(10, -coalesce(p1.decimals, p11.decimals))*coalesce(p1.price, p11.price)*sum(token_1_transfer) over (partition by cow_amm_nb order by time ASC) as token_1_balance_usd, 
        token_2_address, token_2_symbol, token_2_transfer, power(10, -coalesce(p2.decimals, p21.decimals))*token_2_transfer*coalesce(p2.price, p21.price) as token_2_transfer_usd,
        sum(token_2_transfer) over (partition by cow_amm_nb order by time ASC) as token_2_balance, 
        power(10, -coalesce(p2.decimals, p21.decimals))*coalesce(p2.price,p21.price)*sum(token_2_transfer) over (partition by cow_amm_nb order by time ASC) as token_2_balance_usd,
        case when tx_hash in (select tx_hash from trades) and token_1_transfer * token_2_transfer <0 then true else false end as isTrade,
        greatest(0, protocol_fee) as protocol_fee, protocol_fee_token,
        case when protocol_fee = -1 then 0 
            when protocol_fee_token = token_1_address then protocol_fee*coalesce(p1.price, p11.price)*power(10, -coalesce(p1.decimals, p11.decimals)) 
            when protocol_fee_token = token_2_address then protocol_fee*coalesce(p2.price, p21.price)*power(10, -coalesce(p2.decimals, p21.decimals))
            end as protocol_fee_usd
        from cow_amms_evolution_temp t
        -- the 2 first tables joined are the price on dune as reported for the chain of the cow amm
        -- the 2 last ones are the price on dune as reported for mainnet, if it is not available on the specific chain.
        left join prices.usd p1 on token_1_address = p1.contract_address and date_trunc('minute',t.time) = p1.minute and p1.blockchain = t.blockchain
        left join prices.usd p2 on token_2_address = p2.contract_address and date_trunc('minute',t.time) = p2.minute and p2.blockchain = t.blockchain
        --the subquery is to force dunesql to compute it this way to optimize performance
        left join (select distinct minute, price, symbol, decimals from prices.usd where blockchain = 'ethereum') p11 on token_1_symbol = p11.symbol  and date_trunc('minute',t.time) = p11.minute 
        left join (select distinct minute, price, symbol, decimals from prices.usd where blockchain = 'ethereum') p21 on token_2_symbol = p21.symbol and date_trunc('minute',t.time) = p21.minute
    )
  
 select * from cow_amms_evolution 
 order by cow_amm_address, time