-- part of a query repo
-- query name: reCLAMM Volume (last 30 days) EURC/USDC - Base
-- query link: https://dune.com/queries/5840650


SELECT * 
FROM "query_5840620(pool='0x12c2de9522f377b86828f6af01f58c046f814d3c', chain='base')"
WHERE day > now() - interval '30' day