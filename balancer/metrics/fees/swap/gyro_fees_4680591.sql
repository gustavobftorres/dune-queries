-- part of a query repo
-- query name: gyro fees
-- query link: https://dune.com/queries/4680591


SELECT
  day,
  SUM(protocol_fee_collected_usd) AS daily_fees,
  SUM(SUM(protocol_fee_collected_usd)) OVER (ORDER BY day) AS cumulative_fees
FROM balancer.protocol_fee
WHERE pool_type = 'ECLP'
GROUP BY day
ORDER BY day DESC