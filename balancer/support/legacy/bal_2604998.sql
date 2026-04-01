-- part of a query repo
-- query name: BAL
-- query link: https://dune.com/queries/2604998


WITH 
    subset AS ( 
        SELECT 
            s.contract_address AS contract_address, 
            s.raw_token_amount, 
            s.raw_token_amount / POWER(10,COALESCE(t.decimals, p.decimals, 18)) AS token_amount,
            t.symbol as t_sym, 
            t.decimals as t_dec, 
            t.blockchain as t_b, 
            p.symbol as p_sym, 
            p.decimals as p_dec, 
            p.blockchain as p_b, 
            p.price as price 
        FROM (
            SELECT contract_address, CAST(SUM(value) AS DOUBLE) AS raw_token_amount FROM erc20_ethereum.evt_Transfer 
            WHERE "from" = 0x0000000000000000000000000000000000000000
            AND contract_address = 0xba100000625a3754423978a60c9317c58a424e3d
            GROUP BY 1
            UNION ALL
            SELECT contract_address, -1 * CAST(SUM(value) AS DOUBLE) AS raw_token_amount FROM erc20_ethereum.evt_Transfer 
            WHERE to = 0x0000000000000000000000000000000000000000
            AND contract_address = 0xba100000625a3754423978a60c9317c58a424e3d
            GROUP BY 1
        ) s 
    LEFT JOIN tokens.erc20 t ON t.contract_address = s.contract_address AND t.blockchain = 'ethereum'
    LEFT JOIN prices.usd p ON p.contract_address = s.contract_address AND p.blockchain = 'ethereum'
    AND minute = date_trunc('hour', NOW()) - INTERVAL '1' HOUR
)
SELECT 
    contract_address, 
    COALESCE(t_sym, p_sym, '???') AS token_symbol, 
    price, 
    SUM(token_amount) * price AS market_cap, 
    SUM(token_amount) AS circ_supply 
FROM subset 
GROUP BY 1, 2, 3
