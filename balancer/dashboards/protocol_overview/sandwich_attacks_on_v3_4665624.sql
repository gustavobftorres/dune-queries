-- part of a query repo
-- query name: Sandwich Attacks on V3
-- query link: https://dune.com/queries/4665624


WITH sandwich_trades AS (
    SELECT front.block_time,
           front.blockchain,
           front.tx_hash AS frontrun_tx_hash,
           back.tx_hash AS backrun_tx_hash,
           front.tx_from AS attacker,
           front.project_contract_address,
           front.token_bought_address,
           front.token_sold_address,
           front.evt_index AS frontrun_evt_index,
           back.evt_index AS backrun_evt_index
    FROM dex.sandwiches front
    INNER JOIN dex.sandwiches back 
        ON front.block_time = back.block_time
        AND front.project_contract_address = back.project_contract_address
        AND front.tx_from = back.tx_from
        AND front.tx_hash != back.tx_hash
        AND front.token_sold_address = back.token_bought_address
        AND front.token_bought_address = back.token_sold_address
        AND front.evt_index + 1 < back.evt_index
        AND front.blockchain = back.blockchain
    WHERE 1 = 1
    AND front.project = 'balancer'
    AND front.block_time >= NOW() - INTERVAL '60' day 
    AND front.version = '3')

SELECT dt.block_time,
       dt.project_contract_address,
       l.name AS pool_symbol,
       dt.blockchain,
       st.attacker,
       st.frontrun_tx_hash,
       CASE 
           WHEN dt.tx_hash = st.frontrun_tx_hash THEN 'frontrun'
           WHEN dt.tx_hash = st.backrun_tx_hash THEN 'backrun'
           ELSE 'sandwiched'
       END AS sandwich_type,
       dt.tx_hash AS trade_tx_hash,
       st.backrun_tx_hash,
       dt.tx_from AS trader,
       dt.token_sold_address,
       dt.token_sold_symbol,
       dt.token_bought_address,
       dt.token_bought_symbol,
       dt.token_sold_amount,
       dt.token_bought_amount,
       dt.amount_usd,
       dt.evt_index
FROM dex.sandwiched dt
INNER JOIN sandwich_trades st 
    ON dt.block_time = st.block_time
    AND dt.blockchain = st.blockchain
    AND dt.project_contract_address = st.project_contract_address
    AND dt.token_bought_address = st.token_bought_address
    AND dt.token_sold_address = st.token_sold_address
    AND dt.evt_index BETWEEN st.frontrun_evt_index AND st.backrun_evt_index
JOIN labels.balancer_v3_pools l 
    ON dt.project_contract_address = l.address
    AND l.blockchain = dt.blockchain
    --AND dt.tx_hash != st.frontrun_tx_hash
