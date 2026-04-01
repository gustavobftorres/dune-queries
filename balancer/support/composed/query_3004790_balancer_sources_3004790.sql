-- part of a query repo
-- query name: (query_3004790) balancer_sources
-- query link: https://dune.com/queries/3004790


/*
queried on:
Balancer Volume by Source and Pool https://dune.com/queries/2718077
Balancer Volume by Source (Volume Breakdown) https://dune.com/queries/2944378
Balancer Volume (Bot Breakdown) (weekly top 5) https://dune.com/queries/2945043
Balancer Volume (Heavy Traders) (weekly top 5) https://dune.com/queries/2945137
Balancer 7-day Volume by Source (Volume Breakdown) https://dune.com/queries/2944392
Balancer 7-day Volume (Bot Breakdown) (daily top 5) https://dune.com/queries/2945205
Balancer 7-day Volume (Heavy Trader Breakdown) https://dune.com/queries/2945207
Balancer 24 hour Volume by Source (Volume Breakdown) https://dune.com/queries/2945243
Balancer 24 hour Volume (Heavy Trader Breakdown) https://dune.com/queries/2945245
Balancer 24 hour Volume (Bot Breakdown) (daily top 5) https://dune.com/queries/2945246
Balancer Volume by Source https://dune.com/queries/2650923
*/
SELECT address, name, blockchain FROM query_2999890 -- avalanche
WHERE address IS NOT NULL
UNION ALL
SELECT address, name, blockchain FROM query_2999836 -- gnosis
WHERE address IS NOT NULL
UNION ALL
SELECT address, name, blockchain FROM query_2999200 -- optimism
WHERE address IS NOT NULL
UNION ALL
SELECT address, name, blockchain FROM query_2998828 -- arbitrum
WHERE address IS NOT NULL
UNION ALL
SELECT address, name, blockchain FROM query_2998457 -- polygon
WHERE address IS NOT NULL
UNION ALL
SELECT address, name, blockchain FROM query_2478528 -- ethereum
WHERE address IS NOT NULL
UNION ALL
SELECT address, name, blockchain FROM query_3033138 -- base
WHERE address IS NOT NULL
UNION ALL
SELECT address, name, blockchain FROM query_3699365 -- zkevm
WHERE address IS NOT NULL