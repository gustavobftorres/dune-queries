-- part of a query repo
-- query name: ve8020 Fees Collected
-- query link: https://dune.com/queries/3151572


SELECT 
    CAST(DATE_TRUNC('week',day) as TIMESTAMP) as week,
    SUM(protocol_fee_collected_usd) as protocol_fee_collected_usd,
    (SELECT SUM(amount_usd)/1e6 FROM balancer.trades
    WHERE project_contract_address = {{Pool Address}}
        AND blockchain = '{{Blockchain}}') as amount_usd,
    SUM(SUM(protocol_fee_collected_usd)) OVER (ORDER BY CAST(DATE_TRUNC('week',day) as TIMESTAMP)) as cumulative_protocol_fee_collected_usd,
    SUM(SUM(protocol_fee_collected_usd)) OVER (ORDER BY CAST(DATE_TRUNC('week',day) as TIMESTAMP))/1e6 as short_cumulative_protocol_fee_collected_usd,
    AVG(protocol_fee_collected_usd)*100 as protocol_fee_collected_usd_perc
FROM balancer.protocol_fee
    WHERE pool_address = {{Pool Address}}
        AND blockchain = '{{Blockchain}}'
GROUP BY 1, pool_address
ORDER BY 1 DESC