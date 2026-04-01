-- part of a query repo
-- query name: reCLAMM Volume (last 30 days) COW/WETH - Base
-- query link: https://dune.com/queries/5840626


SELECT * 
FROM "query_5840620(pool='0xff028c1ec4559d3aa2b0859aa582925b5cc28069', chain='base')"
WHERE day > now() - interval '30' day