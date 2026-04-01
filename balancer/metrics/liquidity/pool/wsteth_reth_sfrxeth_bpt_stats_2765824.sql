-- part of a query repo
-- query name: wstETH-rETH-sfrxETH-BPT Stats
-- query link: https://dune.com/queries/2765824


WITH tvl as(
SELECT CAST(day as timestamp) as day, sum(usd_amount) as liquidity FROM balancer_v2_ethereum.liquidity WHERE pool_id = '0x42ed016f826165c2e5976fe5bc3df540c5ad0af700000000000000000000058b'
GROUP BY 1),

swaps as(
SELECT date_trunc('day',block_date) as day, sum(amount_usd) AS volume FROM dex.trades WHERE project_contract_address = 0x42ed016f826165c2e5976fe5bc3df540c5ad0af7
GROUP BY 1
)

SELECT s.day, s.volume, t.liquidity 
FROM swaps s
LEFT JOIN tvl t ON t.day = s.day