-- part of a query repo
-- query name: Humpy's monthly BAL bought
-- query link: https://dune.com/queries/3861074


WITH address_set AS (
    SELECT wallet_address
    FROM query_3859543
),
transfers_in AS (
    SELECT
        t.evt_block_time,
        t."to" AS wallet_address,
        t.value / 1e18 AS value_in_currency
    FROM
        erc20_ethereum.evt_Transfer t
    WHERE
        t.contract_address = 0xba100000625a3754423978a60c9317c58a424e3D
        AND t."to" IN (SELECT wallet_address FROM address_set)
        AND DATE_TRUNC('month', t.evt_block_time) = DATE_TRUNC('month', CURRENT_DATE)
)
SELECT 
    SUM(value_in_currency) AS total_bal_in
FROM 
    transfers_in;
