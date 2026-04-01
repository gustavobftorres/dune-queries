-- part of a query repo
-- query name: Balancer CoWSwap AMM total surplus
-- query link: https://dune.com/queries/3957751


WITH pools AS (
    SELECT 
        bPool AS pools
    FROM b_cow_amm_ethereum.BCoWFactory_evt_LOG_NEW_POOL
),

transfers AS (
    SELECT 
        p.pools AS pool, 
        e.evt_block_time, 
        e.contract_address AS token, 
        (value * u.price) / POWER (10, u.decimals) AS amount
    FROM erc20_ethereum.evt_transfer e
    INNER JOIN pools p ON e."to" = p.pools
    LEFT JOIN prices.usd u ON u.contract_address = e.contract_address 
    AND u.minute = DATE_TRUNC('minute', e.evt_block_time)
    AND u.blockchain = 'ethereum'
)

SELECT * FROM transfers