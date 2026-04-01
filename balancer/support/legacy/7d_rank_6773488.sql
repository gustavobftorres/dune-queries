-- part of a query repo
-- query name: 7D Rank
-- query link: https://dune.com/queries/6773488


SELECT
    ROW_NUMBER() OVER (ORDER BY vol DESC) AS seven_day_vol_rank,
    vol AS seven_day_volume,
    blockchain,
    pool
FROM (
    SELECT DISTINCT
        project_contract_address AS pool,
        blockchain,
        SUM(amount_usd) OVER (PARTITION BY project_contract_address, blockchain) AS vol
    FROM balancer.trades
    WHERE block_time > NOW() - INTERVAL '7' day
) t