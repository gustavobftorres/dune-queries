-- part of a query repo
-- query name: Humpy's AURABAL Balance
-- query link: https://dune.com/queries/3861547


WITH address_set AS (
    SELECT wallet_address
    FROM query_3859543
),
transfers AS (
    SELECT
        t.evt_block_time,
        t."from" AS wallet_address,
        -t.value / 1e18 AS value_in_currency,
        'BAL' AS currency_type
    FROM
        erc20_ethereum.evt_Transfer t
    WHERE
        t.contract_address = 0x616e8BfA43F920657B3497DBf40D6b1A02D4608d
        AND t."from" IN (SELECT wallet_address FROM address_set)
    
    UNION ALL
    
    SELECT
        t.evt_block_time,
        t."to" AS wallet_address,
        t.value / 1e18 AS value_in_currency,
        'BAL' AS currency_type
    FROM
        erc20_ethereum.evt_Transfer t
    WHERE
        t.contract_address = 0xba100000625a3754423978a60c9317c58a424e3D
        AND t."to" IN (SELECT wallet_address FROM address_set)
),
daily_balances AS (
    SELECT 
        DATE_TRUNC('day', t.evt_block_time) AS day,
        SUM(t.value_in_currency) AS daily_net_change
    FROM 
        transfers t
    GROUP BY 
        DATE_TRUNC('day', t.evt_block_time)
),
cumulative_balances AS (
    SELECT
        day,
        SUM(daily_net_change) OVER (ORDER BY day) AS cumulative_balance
    FROM
        daily_balances
)
SELECT 
    day,
    cumulative_balance
FROM 
    cumulative_balances
ORDER BY 
    day ASC;
