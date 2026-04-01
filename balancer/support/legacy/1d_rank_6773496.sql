-- part of a query repo
-- query name: 1D Rank
-- query link: https://dune.com/queries/6773496


SELECT
    ROW_NUMBER() OVER (ORDER BY vol DESC) AS one_day_vol_rank,
    vol AS one_day_volume,
    blockchain,
    pool
FROM (
    SELECT DISTINCT
        project_contract_address AS pool,
        blockchain,
        SUM(amount_usd) OVER (PARTITION BY project_contract_address, blockchain) AS vol
    FROM balancer.trades
    WHERE block_time > NOW() - INTERVAL '1' day
) t