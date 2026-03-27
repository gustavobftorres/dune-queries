-- part of a query repo
-- query name: reCLAMM Volume (last 30 days) USDC.e/GNO - Gnosis
-- query link: https://dune.com/queries/5840641


SELECT * 
FROM "query_5840620(pool='0x70b3b56773ace43fe86ee1d80cbe03176cbe4c09', chain='gnosis')"
WHERE day > now() - interval '30' day