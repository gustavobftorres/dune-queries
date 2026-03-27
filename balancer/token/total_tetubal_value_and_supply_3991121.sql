-- part of a query repo
-- query name: Total tetuBAL value and Supply
-- query link: https://dune.com/queries/3991121


    SELECT
  SUM(value) / CAST(1e18 AS DOUBLE) AS total_supply,
  (
    SUM(value) / CAST(1e18 AS DOUBLE) * (
        SELECT median_price
        FROM
        dex.prices
        WHERE
        contract_address = 0x7fc9e0aa043787bfad28e29632ada302c790ce33
        AND blockchain = 'polygon'
        ORDER BY hour DESC
        LIMIT 1
    )
  ) as total_usd
FROM
  erc20_polygon.evt_Transfer
WHERE
  contract_address = 0x7fc9e0aa043787bfad28e29632ada302c790ce33
  AND "from" = 0x0000000000000000000000000000000000000000