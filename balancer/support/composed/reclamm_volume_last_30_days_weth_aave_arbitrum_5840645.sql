-- part of a query repo
-- query name: reCLAMM Volume (last 30 days) WETH/AAVE - Arbitrum
-- query link: https://dune.com/queries/5840645


SELECT * 
FROM "query_5840620(pool='0x5ea58d57952b028c40bd200e5aff20fc4b590f51', chain='arbitrum')"
WHERE day > now() - interval '30' day