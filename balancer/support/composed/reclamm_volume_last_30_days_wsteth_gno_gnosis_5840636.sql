-- part of a query repo
-- query name: reCLAMM Volume (last 30 days) wstETH/GNO - Gnosis
-- query link: https://dune.com/queries/5840636


SELECT * 
FROM "query_5840620(pool='0xa50085ff1dfa173378e7d26a76117d68d5eba539', chain='gnosis')"
WHERE day > now() - interval '30' day