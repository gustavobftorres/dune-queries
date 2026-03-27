-- part of a query repo
-- query name: Aura Total auraBAL value and Supply
-- query link: https://dune.com/queries/3990850


    SELECT
  SUM(value) / CAST(1e18 AS DOUBLE) AS total_supply,
  (
    SUM(value) / CAST(1e18 AS DOUBLE) * (
        SELECT token_price_usd
        FROM
        dex.prices_latest
        WHERE
        token_address = 0x616e8BfA43F920657B3497DBf40D6b1A02D4608d
        ORDER BY block_time DESC
        LIMIT 1
    )
  ) as total_usd
FROM
  erc20_ethereum.evt_Transfer
WHERE
  contract_address = 0x616e8BfA43F920657B3497DBf40D6b1A02D4608d
  AND "from" = 0x0000000000000000000000000000000000000000