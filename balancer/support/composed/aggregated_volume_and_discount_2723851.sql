-- part of a query repo
-- query name: Aggregated Volume and Discount
-- query link: https://dune.com/queries/2723851


SELECT 
day, SUM(Volume) OVER (ORDER BY day ASC) as cumulative_vol, SUM("Aggregate Delta") OVER (ORDER BY day ASC) as cumulative_disc
FROM query_2720447