-- part of a query repo
-- query name: LST Pools Stats
-- query link: https://dune.com/queries/3380377


SELECT 
    SUM(volume_24h)/1e6 AS volume,
    SUM(volume_30d)/1e9 AS volume_30d,
    SUM(volume_all_time)/1e9 AS volume_all_time,
    SUM(TVL)/1e6 AS tvl, 
    SUM(fees_collected)/1e6 AS fees_collected,
    SUM(fees_collected_30d)/1e3 AS fees_collected_30d,
    SUM(volume_24h_eth)/1e3 AS volume_eth,
    SUM(volume_30d_eth)/1e3 AS volume_30d_eth,
    SUM(volume_all_time_eth)/1e6 AS volume_all_time_eth,
    SUM(TVL_eth)/1e3 AS tvl_eth, 
    SUM(fees_collected_eth)/1e3 AS fees_collected_eth,
    SUM(fees_collected_30d_eth) AS fees_collected_30d_eth
FROM query_3375874
WHERE ('{{3. Blockchain}}' = 'All' OR long_chain = '{{3. Blockchain}}')