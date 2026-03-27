-- part of a query repo
-- query name: reCLAMM Volume (last 30 days) COW/WETH - Ethereum
-- query link: https://dune.com/queries/5840632


SELECT * 
FROM "query_5840620(pool='0xd321300ef77067d4a868f117d37706eb81368e98', chain='ethereum')"
WHERE day > now() - interval '30' day