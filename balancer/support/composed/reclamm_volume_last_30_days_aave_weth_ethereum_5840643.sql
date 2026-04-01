-- part of a query repo
-- query name: reCLAMM Volume (last 30 days) AAVE/WETH - Ethereum
-- query link: https://dune.com/queries/5840643


SELECT * 
FROM "query_5840620(pool='0x9d1fcf346ea1b073de4d5834e25572cc6ad71f4d', chain='ethereum')"
WHERE day > now() - interval '30' day