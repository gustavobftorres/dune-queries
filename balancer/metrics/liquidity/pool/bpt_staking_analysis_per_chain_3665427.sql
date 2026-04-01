-- part of a query repo
-- query name: BPT Staking Analysis per Chain
-- query link: https://dune.com/queries/3665427


WITH 
gauge_to_pool AS(
        SELECT 
            blockchain,
            pool_address,
            gauge_address,
            gauge_name
        FROM dune.balancer.result_gauge_to_pool_mapping
),

transfers AS (
        SELECT
            t.block_date AS day,
            t.blockchain,
            t.contract_address AS token,
            COALESCE(SUM(CASE WHEN t."from" = g.gauge_address THEN value / POWER(10, 18) ELSE 0 END), 0) AS unstake,
            COALESCE(SUM(CASE WHEN t.to = g.gauge_address THEN value / POWER(10, 18) ELSE 0 END), 0) AS stake
        FROM balancer.transfers_bpt t
        INNER JOIN gauge_to_pool g ON t.blockchain = g.blockchain 
        AND t.contract_address = g.pool_address
        WHERE t.block_date <= (SELECT MAX(day) FROM balancer.bpt_supply)
        /*AND t.block_date >= TIMESTAMP '{{Start Date}}'
        AND t.block_date <= TIMESTAMP '{{End Date}}'*/
        AND t.blockchain = 'ethereum'
        GROUP BY 1, 2, 3
    ),

    balances AS (
        SELECT
            day,
            blockchain,
            token,
            SUM(COALESCE(stake, 0) - COALESCE(unstake, 0)) OVER (PARTITION BY token, blockchain ORDER BY DAY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS staked
        FROM transfers
    )
    
    SELECT 
    CAST(b.day AS TIMESTAMP) AS day,
    SUM(b.staked) AS staked,
    SUM(s.supply) AS supply,
    SUM(b.staked) / SUM(s.supply) AS pct_staked
    FROM balances b
    INNER JOIN balancer.bpt_supply s 
    ON s.token_address = b.token 
    AND s.blockchain = b.blockchain
    AND s.day = b.day
    LEFT JOIN labels.balancer_v2_pools l 
    ON l.blockchain = b.blockchain
    AND l.address = b.token
    WHERE /*'{{Blockchain}}' = 'All' OR b.blockchain = '{{Blockchain}}'
    AND*/ '{{Pool Address}}' = 'All' OR CAST(b.token AS VARCHAR) = LOWER('{{Pool Address}}')
    AND '{{Pool Type}}' = 'All' OR l.pool_type = '{{Pool Type}}'
    GROUP BY 1
    ORDER BY 1 DESC