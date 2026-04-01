-- part of a query repo
-- query name: BAL Circulating Supply
-- query link: https://dune.com/queries/2602212


WITH 
    token_balance AS (
        SELECT CAST(SUM(value/POWER(10,18)) AS DOUBLE) AS token_value FROM erc20_ethereum.evt_Transfer 
        WHERE contract_address = 0xba100000625a3754423978a60c9317c58a424e3d 
        AND "from" = 0x0000000000000000000000000000000000000000
        UNION ALL
        SELECT -1 * CAST(SUM(value/POWER(10,18)) AS DOUBLE) AS token_value FROM erc20_ethereum.evt_Transfer 
        WHERE contract_address = 0xba100000625a3754423978a60c9317c58a424e3d 
        AND to = 0x0000000000000000000000000000000000000000
    )
select SUM(token_value) AS token_supply FROM token_balance
