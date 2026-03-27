-- part of a query repo
-- query name: mev hook reverted txns
-- query link: https://dune.com/queries/5174507


WITH aggregators AS (
  SELECT * FROM (
    VALUES 
      (0x9008D19f58AAbD9eD0D60971565AA8510560ab41, 'CoW'),
      (0x19cEeAd7105607Cd444F5ad10dd51356436095a1, 'Odos'),
      (0x111111125421cA6dc452d289314280a0f8842A65, '1inch'),
      (0x6131B5fae19EA4f9D964eAc0408E4408b66337b5, 'Kyber'),
      (0x6A000F20005980200259B80c5102003040001068, 'ParaSwap'),
      (0x0000000000001fF3684f28c67538d4D072C22734, '0x/Matcha')
  ) AS t (address, label)
)
SELECT
  a.label as aggregator,
  sum(case when m.output_1 > 100000000000000 then 1 else 0 end) as mev_taxed_count,
  sum(case when m.output_1 <= 100000000000000 then 1 else 0 end) as not_mev_taxed_count,
  count(*) as total_count,
  round(100.0 * sum(case when m.output_1 > 100000000000000 then 1 else 0 end) / count(*), 2) as mev_taxed_percentage
FROM balancer_v3_multichain.mevcapturehook_call_oncomputedynamicswapfeepercentage m
INNER JOIN aggregators a ON m.call_tx_to = a.address
WHERE m.call_success = false
GROUP BY a.label
ORDER BY total_count DESC
