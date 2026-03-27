-- part of a query repo
-- query name: Total sdBAL value and Supply
-- query link: https://dune.com/queries/3991064


    SELECT
  SUM(value) / CAST(1e18 AS DOUBLE) AS total_supply,
  (
    SUM(value) / CAST(1e18 AS DOUBLE) * (
        SELECT median_price
        FROM
        query_3993627
        ORDER BY day DESC
        LIMIT 1
    )
  ) as total_usd
FROM
  erc20_ethereum.evt_Transfer
WHERE
  contract_address = 0xf24d8651578a55b0c119b9910759a351a3458895
  AND "from" = 0x0000000000000000000000000000000000000000