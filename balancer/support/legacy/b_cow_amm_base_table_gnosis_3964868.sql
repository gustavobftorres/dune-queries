-- part of a query repo
-- query name: b_cow_amm_base_table_gnosis
-- query link: https://dune.com/queries/3964868


with
  gnosis_cow_amms_temp AS (
    SELECT 
        a.contract_address AS cow_amm_address,
        a.token AS token_1_address,
        b.token AS token_2_address
    FROM b_cow_amm_gnosis.BCoWPool_call_bind a
    JOIN b_cow_amm_gnosis.BCoWPool_call_bind b
    ON a.contract_address = b.contract_address
    WHERE a.token < b.token
    AND a.call_success),
  
 gnosis_cow_amms AS (
    SELECT
        'gnosis' AS  blockchain,
        a.cow_amm_address,
        a.token_1_address,
        b.symbol AS token_1_symbol,
        a.token_2_address,
        c.symbol AS token_2_symbol
    FROM gnosis_cow_amms_temp a
     JOIN tokens.erc20 b ON a.token_1_address = b.contract_address
     JOIN tokens.erc20 c ON a.token_2_address = c.contract_address
  ),

  cow_amms_temp AS (
      SELECT * FROM gnosis_cow_amms
  ),
  
  cow_amms AS (
    SELECT 
        *, 
        COUNT(cow_amm_address) OVER (ORDER BY cow_amm_address, token_1_address, token_2_address) AS cow_amm_nb 
    FROM cow_amms_temp
  ),

--all the net transfers to/FROM the cow amms
transfers_temp AS (
    SELECT 
        'gnosis' AS blockchain, 
        contract_address AS token_address,
        (CASE WHEN "FROM" IN (SELECT cow_amm_address FROM gnosis_cow_amms_temp) THEN "FROM" ELSE to END) AS cow_amm_address,
        (CASE WHEN "FROM" IN (SELECT cow_amm_address FROM gnosis_cow_amms_temp) THEN -value ELSE value END) AS net_value,
        evt_block_time AS time, evt_tx_hash AS tx_hash
      FROM erc20_gnosis.evt_transfer
      WHERE ("FROM" IN (SELECT cow_amm_address FROM gnosis_cow_amms)
        OR "to" IN (SELECT cow_amm_address FROM gnosis_cow_amms))
        AND evt_block_time > TIMESTAMP '2024-07-01'

    --add WXDAI deposit/withdrawals
    UNION 
    
    SELECT 
          'gnosis' AS blockchain, 
          0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d AS token_address,
          varbinary_substring(topic1,13,20) AS cow_amm_address, 
          varbinary_to_uint256(data) AS net_value,
          block_time AS time, 
          tx_hash
    FROM gnosis.logs
    WHERE varbinary_substring(topic1,13,20) in (SELECT cow_amm_address FROM gnosis_cow_amms)
    AND contract_address = 0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d
    AND topic0 = 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c
    AND block_time > TIMESTAMP '2024-07-01'  
      
    UNION
    
    SELECT 
        'gnosis' AS blockchain, 
        0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d AS token_address,
        varbinary_substring(topic1,13,20) AS cow_amm_address,  
        -varbinary_to_uint256(data) AS net_value,
        block_time AS time, 
        tx_hash
      FROM gnosis.logs
      WHERE varbinary_substring(topic1,13,20) in (SELECT cow_amm_address FROM gnosis_cow_amms)
      AND contract_address = 0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d
      AND topic0 = 0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65
      AND block_time > TIMESTAMP '2024-07-01'  
      
    --add sDAI deposit/withdrawals
    UNION 
    
    SELECT 
        'gnosis' AS blockchain, 
        0xaf204776c7245bF4147c2612BF6e5972Ee483701 AS token_address,
        varbinary_substring(topic1,13,20) AS cow_amm_address, 
        varbinary_to_uint256(varbinary_substring(data,33,32)) AS net_value,
        block_time AS time, 
        tx_hash
     FROM gnosis.logs
     WHERE varbinary_substring(topic1,13,20) in (SELECT cow_amm_address FROM gnosis_cow_amms)
     AND contract_address = 0xaf204776c7245bF4147c2612BF6e5972Ee483701
     AND topic0 = 0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7
     AND block_time > TIMESTAMP '2024-07-01'  
     
    UNION
    
    SELECT 
        'gnosis' AS blockchain, 
        0xaf204776c7245bF4147c2612BF6e5972Ee483701 AS token_address,
        varbinary_substring(topic1,13,20) AS cow_amm_address, 
        -varbinary_to_uint256(varbinary_substring(data,33,32)) AS net_value,
        block_time AS time, 
        tx_hash
    FROM gnosis.logs
    WHERE varbinary_substring(topic1,13,20) in (SELECT cow_amm_address FROM gnosis_cow_amms)
    AND contract_address = 0xaf204776c7245bF4147c2612BF6e5972Ee483701
    AND topic0 = 0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db
    AND block_time > TIMESTAMP '2024-07-01'  
  ),
  
   transfers AS(
        SELECT 
            blockchain, 
            token_address, 
            cow_amm_address, 
            SUM(net_value) AS net_value, 
            time, 
            tx_hash
        FROM transfers_temp
        GROUP BY blockchain, token_address, cow_amm_address, time, tx_hash
    ),
  
  --beware of this filter if the cowamm was created multiple times (with different pairs)
    transfers_token_1 AS(
        SELECT 
            t.blockchain, 
            t.cow_amm_address, 
            a.cow_amm_nb, 
            time, 
            tx_hash,token_address AS token_1_address, 
            token_2_address, 
            net_value AS token_1_transfer
        FROM transfers t
        JOIN cow_amms a ON t.cow_amm_address = a.cow_amm_address AND t.blockchain = a.blockchain
        AND token_address = token_1_address
    ),
    transfers_token_2 AS(
        SELECT 
            t.blockchain, 
            t.cow_amm_address, 
            a.cow_amm_nb, 
            time, 
            tx_hash,
            token_address AS token_2_address, 
            token_1_address, 
            net_value AS token_2_transfer
        FROM transfers t
        JOIN cow_amms a ON t.cow_amm_address = a.cow_amm_address AND t.blockchain = a.blockchain
        AND token_address = token_2_address
    ),
    
     transfers_in_line AS(
        SELECT 
            COALESCE(t1.blockchain, t2.blockchain) AS blockchain,
            COALESCE(t1.cow_amm_address, t2.cow_amm_address) AS cow_amm_address,
            COALESCE(t1.time, t2.time) AS time,
            COALESCE(t1.tx_hash, t2.tx_hash) AS tx_hash,
            COALESCE(t1.token_1_address, t2.token_1_address) AS token_1_address, 
            COALESCE(t1.token_2_address, t2.token_2_address) AS token_2_address, 
            COALESCE(t1.token_1_transfer, 0) AS token_1_transfer,
            COALESCE(t2.token_2_transfer, 0) AS token_2_transfer
        FROM cow_amms a
        JOIN transfers_token_1 t1 ON t1.cow_amm_nb = a.cow_amm_nb
        FULL OUTER JOIN transfers_token_2 t2 ON t2.cow_amm_nb = a.cow_amm_nb AND t1.time = t2.time
        ),
  
  trades AS(
        SELECT 
            'gnosis' AS blockchain, 
            t.tx_hash, 
            t.order_uid, 
            t.trader AS cow_amm_address,
            cast(r.data.protocol_fee AS int256) AS protocol_fee,
            from_hex(r.data.protocol_fee_token) AS protocol_fee_token,
            least(buy_token_address, sell_token_address) AS token_1_address, greatest(buy_token_address, sell_token_address) AS token_2_address,
            t.block_time AS time
        FROM cow_protocol_gnosis.trades t
        LEFT JOIN cowswap.raw_order_rewards r ON cast(r.order_uid AS VARCHAR) = cast(t.order_uid AS VARCHAR)
        WHERE trader in (SELECT cow_amm_address FROM gnosis_cow_amms)
    ),
    
   cow_amms_evolution_temp AS(
        SELECT 
            a.blockchain, 
            a.cow_amm_address, 
            a.cow_amm_nb, 
            a.token_1_address, 
            token_1_symbol, 
            a.token_2_address, 
            token_2_symbol, 
            l.token_1_transfer, 
            l.token_2_transfer, 
            l.time, 
            l.tx_hash,
        -- for protocol_fee, the -1 is a trick used in the next table to show that no fees WHERE applied, AND therefore protocol_fee_token is null
            COALESCE(t.protocol_fee, -1) AS protocol_fee, 
            t.protocol_fee_token
        FROM cow_amms a
        JOIN transfers_in_line l ON a.cow_amm_address = l.cow_amm_address 
        AND a.blockchain = l.blockchain
        AND l.token_1_address = a.token_1_address 
        AND l.token_1_address = a.token_1_address
        LEFT JOIN trades t ON a.cow_amm_address = t.cow_amm_address 
        AND l.tx_hash = t.tx_hash 
        AND l.blockchain = t.blockchain 
        AND t.token_1_address = a.token_1_address 
        AND t.token_1_address = a.token_1_address
    ),
    
    cow_amms_evolution AS (
        SELECT 
            t.blockchain, 
            cow_amm_address, 
            cow_amm_nb, 
            time, 
            tx_hash, 
            token_1_address, 
            token_1_symbol, 
            token_1_transfer, 
            POWER(10, -COALESCE(p1.decimals, p11.decimals))*token_1_transfer*COALESCE(p1.price, p11.price) AS token_1_transfer_usd,
            SUM(token_1_transfer) OVER (partition BY cow_amm_nb ORDER BY time ASC) AS token_1_balance, 
            POWER(10, -COALESCE(p1.decimals, p11.decimals))*COALESCE(p1.price, p11.price)*SUM(token_1_transfer) OVER (partition BY cow_amm_nb ORDER BY time ASC) AS token_1_balance_usd, 
            token_2_address, token_2_symbol, token_2_transfer, POWER(10, -COALESCE(p2.decimals, p21.decimals))*token_2_transfer*COALESCE(p2.price, p21.price) AS token_2_transfer_usd,
            SUM(token_2_transfer) OVER (partition BY cow_amm_nb ORDER BY time ASC) AS token_2_balance, 
            POWER(10, -COALESCE(p2.decimals, p21.decimals))*COALESCE(p2.price,p21.price)*SUM(token_2_transfer) OVER (partition BY cow_amm_nb ORDER BY time ASC) AS token_2_balance_usd,
            CASE WHEN tx_hash in (SELECT tx_hash FROM trades) AND token_1_transfer * token_2_transfer <0 THEN true 
                ELSE false 
            END AS isTrade,
            greatest(0, protocol_fee) AS protocol_fee, protocol_fee_token,
            CASE WHEN protocol_fee = -1 THEN 0 
                WHEN protocol_fee_token = token_1_address THEN protocol_fee*COALESCE(p1.price, p11.price)*POWER(10, -COALESCE(p1.decimals, p11.decimals)) 
                WHEN protocol_fee_token = token_2_address THEN protocol_fee*COALESCE(p2.price, p21.price)*POWER(10, -COALESCE(p2.decimals, p21.decimals))
            END AS protocol_fee_usd
        FROM cow_amms_evolution_temp t
        -- the 2 first tables joined are the price ON dune AS reported for the chain of the cow amm
        -- the 2 last ones are the price ON dune AS reported for mainnet, if it is not available ON the specific chain.
        LEFT JOIN prices.usd p1 ON token_1_address = p1.contract_address 
        AND date_trunc('minute',t.time) = p1.minute 
        AND p1.blockchain = t.blockchain
        LEFT JOIN prices.usd p2 ON token_2_address = p2.contract_address 
        AND date_trunc('minute',t.time) = p2.minute 
        AND p2.blockchain = t.blockchain
        --the subquery is to force dunesql to compute it this way to optimize performance
        LEFT JOIN (SELECT distinct minute, price, symbol, decimals FROM prices.usd WHERE blockchain = 'gnosis') p11 ON token_1_symbol = p11.symbol  AND date_trunc('minute',t.time) = p11.minute 
        LEFT JOIN (SELECT distinct minute, price, symbol, decimals FROM prices.usd WHERE blockchain = 'gnosis') p21 ON token_2_symbol = p21.symbol AND date_trunc('minute',t.time) = p21.minute
    )
  
 SELECT * FROM cow_amms_evolution 
 ORDER BY cow_amm_address, time