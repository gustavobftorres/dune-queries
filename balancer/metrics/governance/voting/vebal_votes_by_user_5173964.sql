-- part of a query repo
-- query name: veBAL Votes by User
-- query link: https://dune.com/queries/5173964


WITH parsed_data AS (
    SELECT
        CAST(address as VARCHAR) as gauge,
        name as label
    FROM labels.balancer_gauges
),

labels AS (
    SELECT
        gauge,
        label AS symbol
    FROM parsed_data
)

SELECT
    provider AS user,
    start_date,
    v.gauge,
    symbol,
    vote
FROM balancer_ethereum.vebal_votes AS v
LEFT JOIN labels l ON l.gauge = CAST(v.gauge AS VARCHAR)
