-- part of a query repo
-- query name: prices_comparison_error
-- query link: https://dune.com/queries/5789026


WITH price_chain_A AS (
    SELECT minute, price, symbol
    FROM prices.usd 
    WHERE 
        blockchain = 'ethereum'
        and symbol = '{{token_a_symbol}}'
        and minute > TIMESTAMP '{{start}}'
),
price_chain_B AS (
    SELECT minute, price
    FROM prices.usd
    WHERE 
        blockchain = 'ethereum' 
        and symbol = '{{token_b_symbol}}'
        and minute > TIMESTAMP '{{start}}'
),
price_aero AS (
    SELECT 
        date_trunc('minute', evt_block_time + interval '30' second) as "minute", 
        MAX(CAST(reserve1 AS DOUBLE)/CAST(reserve0 AS DOUBLE)) as price
    FROM aerodrome_base.pool_evt_sync 
    WHERE contract_address = {{pool_aero}}
       AND evt_block_time > TIMESTAMP '{{start}}'
    GROUP BY date_trunc('minute', evt_block_time + interval '30' second)
),
price_reclamm AS (
    SELECT 
        date_trunc('minute', T.block_time + interval '30' second) as "minute",
        CASE
            WHEN T.token_bought_symbol = '{{token_a_symbol}}' THEN CAST(T.token_sold_amount AS DOUBLE) / CAST(T.token_bought_amount AS DOUBLE)
            ELSE CAST(T.token_bought_amount AS DOUBLE) / CAST(T.token_sold_amount AS DOUBLE)
        END AS price
    FROM balancer_v3_multichain.vault_evt_swap S
    JOIN dex.trades T ON T.block_time > TIMESTAMP '{{start}}' AND T.project = 'balancer' AND T.blockchain = S.chain AND S.evt_tx_hash = T.tx_hash and T.evt_index = S.evt_index
    WHERE S.pool = {{pool_balancer}} AND S.evt_block_time > TIMESTAMP '{{start}}'
)
SELECT
    A."minute",
    100*((PA.price * CAST(B.price AS DOUBLE)) - CAST(A.price AS DOUBLE))/CAST(A.price AS DOUBLE) as "Aero Error %",
    100*((PR.price * CAST(B.price AS DOUBLE)) - CAST(A.price AS DOUBLE))/CAST(A.price AS DOUBLE) as "reCLAMM Error %"
FROM price_chain_A AS A
JOIN price_chain_B AS B on A."minute" = B."minute"
LEFT JOIN price_aero AS PA on A."minute" = PA."minute"
LEFT JOIN price_reclamm AS PR on A."minute" = PR."minute"
ORDER BY "minute"