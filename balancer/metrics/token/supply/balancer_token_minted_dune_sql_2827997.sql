-- part of a query repo
-- query name: Balancer Token Minted (Dune SQL)
-- query link: https://dune.com/queries/2827997


WITH supply AS (
    SELECT date_trunc('day', evt_block_time) AS day,
        SUM(value/1e18) AS mint
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0xba100000625a3754423978a60c9317c58a424e3d
    AND "from" = 0x0000000000000000000000000000000000000000
    GROUP BY 1
)

SELECT day, SUM(mint) OVER (ORDER BY day) FROM supply