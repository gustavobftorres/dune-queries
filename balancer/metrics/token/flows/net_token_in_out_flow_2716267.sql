-- part of a query repo
-- query name: Net Token In/Out Flow
-- query link: https://dune.com/queries/2716267


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
            WHERE block_date >= current_date - interval '{{Date Range in Days}}' day
            GROUP BY 1,2,3
            UNION ALL
            SELECT 
                blockchain,
                block_date,
                token_sold_address AS token,
                sum(amount_usd) AS vol
            FROM balancer.trades
            WHERE block_date >= current_date - interval '{{Date Range in Days}}' day
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
        WHERE r > {{Net Flow Rank}} AND net_flow > 0
    ), 
    others_neg AS (
        SELECT
        distinct
            block_date,
            blockchain,
            'others outflow' AS token,
            sum(net_flow) OVER(PARTITION BY block_date) AS net_flow
        FROM ranks
        WHERE r > {{Net Flow Rank}} AND net_flow < 0
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
        WHERE r <= {{Net Flow Rank}}
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
                ELSE
                    CASE 
                        WHEN blockchain = 'arbitrum' THEN '🟦 |'
                        WHEN blockchain = 'avalanche_c' THEN '⬜ |'
                        WHEN blockchain = 'base' THEN '🟨 |'
                        WHEN blockchain = 'ethereum' THEN 'Ξ  |'
                        WHEN blockchain = 'gnosis' THEN '🟩 |'
                        WHEN blockchain = 'optimism' THEN '🔴 |'
                        WHEN blockchain = 'polygon' THEN '🟪 |'
                END || ' ' || ' ' || token
            END AS sym
        FROM ranked_and_others
    )

SELECT *
    , CAST(block_date AS TIMESTAMP) AS block_date_timestamp
    , CASE
        WHEN net_flow > 0 THEN -1 * ROW_NUMBER() OVER(PARTITION BY block_date ORDER BY net_flow DESC) 
        ELSE ROW_NUMBER() OVER(PARTITION BY block_date ORDER BY net_flow DESC) 
    END AS ro 
    , CASE WHEN month(current_date) < 10 THEN substring(date_format(CAST(block_date AS TIMESTAMP), '%m-%d'), 2, 4)
           ELSE date_format(CAST(block_date AS TIMESTAMP), '%m-%d') 
    END AS formatted_block_date
FROM unique_token_symbols 
ORDER BY block_date DESC, net_flow ASC --, ro ASC