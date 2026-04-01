-- part of a query repo
-- query name: Net Token In/Out Flow
-- query link: https://dune.com/queries/2732279


WITH 
net_flows AS (
    SELECT
        blockchain,
        block_date,
        token,
        sum(vol) AS net_flow
    FROM (
        SELECT 
            blockchain,
            block_date,
            token_bought_address AS token,
            sum(amount_usd) * -1 AS vol
        FROM balancer.trades
        WHERE block_date >= current_date - interval '7' day
        GROUP BY 1,2,3
        UNION ALL
        SELECT 
            blockchain,
            block_date,
            token_sold_address AS token,
            sum(amount_usd) AS vol
        FROM balancer.trades
        WHERE block_date >= current_date - interval '7' day
        GROUP BY 1,2,3
    )
    GROUP BY 1,2,3
), 
ranks AS (
    SELECT *, 
    row_number() OVER(PARTITION BY block_date ORDER BY abs(net_flow) DESC) AS r 
    FROM net_flows
), 
others_pos AS (
    SELECT
    distinct
        block_date,
        blockchain,
        'others inflow' AS token,
        sum(net_flow) OVER(PARTITION BY block_date) AS net_flow
    FROM ranks
    WHERE r > 6 AND net_flow > 0
), 
others_neg AS (
    SELECT
    distinct
        block_date,
        blockchain,
        'others outflow' AS token,
        sum(net_flow) OVER(PARTITION BY block_date) AS net_flow
    FROM ranks
    WHERE r > 6 AND net_flow < 0
), 
ranked_and_others as (
    SELECT 
        r.block_date, 
        r.blockchain, 
        COALESCE(
            t.symbol, 
            substring(CAST(token AS VARCHAR), 1, 3) || '...' || substring(CAST(token AS VARCHAR), 39, 42)
        ) AS token, 
        r.net_flow
    FROM ranks r
    LEFT JOIN tokens.erc20 t ON r.blockchain = t.blockchain AND t.contract_address = r.token
    WHERE r <= 6
    UNION ALL
    SELECT 
        block_date, 
        blockchain, 
        token, 
        net_flow
    FROM others_pos
    UNION ALL
    SELECT 
        block_date, 
        blockchain, 
        token, 
        net_flow
    FROM others_neg
),
unique_token_symbols AS (
    SELECT distinct 
        block_date, 
        token, 
        net_flow, 
        CASE 
            WHEN token = 'others inflow' THEN token
            WHEN token = 'others outflow' THEN token
            ELSE token || ' | ' || substring(blockchain, 1, 3) 
        END AS sym
    FROM ranked_and_others
)

SELECT *, 
    CASE
        WHEN net_flow > 0 THEN -1 * ROW_NUMBER() OVER(PARTITION BY block_date ORDER BY net_flow DESC) 
        ELSE ROW_NUMBER() OVER(PARTITION BY block_date ORDER BY net_flow DESC) 
    END AS ro 
FROM unique_token_symbols 
ORDER BY block_date DESC, ro ASC