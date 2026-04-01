-- part of a query repo
-- query name: Humpy's weekly AURABAL balance delta
-- query link: https://dune.com/queries/3861550


WITH address_set AS (
    SELECT wallet_address
    FROM query_3859543
),
transfers_out AS (
    SELECT
        DATE_TRUNC('week', t.evt_block_time) AS week,
        t."from" AS wallet_address,
        t.value / 1e18 AS value_in_currency_out
    FROM
        erc20_ethereum.evt_Transfer t
    WHERE
        t.contract_address = 0x616e8BfA43F920657B3497DBf40D6b1A02D4608d
        AND t."from" IN (SELECT wallet_address FROM address_set)
        AND t.evt_block_time > NOW() - INTERVAL '6' month
),

transfers_in AS (
    SELECT
        DATE_TRUNC('week', t.evt_block_time) AS week,
        t."to" AS wallet_address,
        t.value / 1e18 AS value_in_currency_in
    FROM
        erc20_ethereum.evt_Transfer t
    WHERE
        t.contract_address = 0x616e8BfA43F920657B3497DBf40D6b1A02D4608d
        AND t."to" IN (SELECT wallet_address FROM address_set)
        AND t.evt_block_time > NOW() - INTERVAL '6' month
),

movements AS(
SELECT 
    week,
    - SUM(value_in_currency_out) AS total_bal_movement
FROM 
    transfers_out o
GROUP BY 1

UNION

SELECT
    week,
    SUM(value_in_currency_in) AS total_bal_movement
FROM
    transfers_in i
GROUP BY 1)
    
SELECT week, SUM(total_bal_movement) AS weekly_balance FROM movements
GROUP BY 1
