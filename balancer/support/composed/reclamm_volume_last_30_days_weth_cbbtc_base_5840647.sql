-- part of a query repo
-- query name: reCLAMM Volume (last 30 days) WETH/cbBTC - Base
-- query link: https://dune.com/queries/5840647


SELECT * 
FROM "query_5840620(pool='0x19aeb8168d921bb069c6771bbaff7c09116720d0', chain='base')"
WHERE day > now() - interval '30' day