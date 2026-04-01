-- part of a query repo
-- query name: BAL + AURA emissions + ARB incentives per gauge, STIP 2nd round
-- query link: https://dune.com/queries/3981342


WITH arb_aura_bal AS (
  SELECT --ARB
    evt_block_time,
    gauge_address,
    'ARB' AS token,
    arb_amount AS amount,
    arb_amount_usd AS amount_usd
  FROM query_3981256

    UNION ALL

  SELECT --AURA
    evt_block_time,
    gauge_address,
    'AURA' AS token,
    amount,
    amount_usd    
  FROM query_3981894
  
     UNION ALL
     
  SELECT --BAL
    emissions_date,
    gauge,
    'BAL' AS token,
    round_emissions,
    round_emissions_usd
  FROM query_3981181)
  
SELECT
  evt_block_time,
  gauge_address AS gauge,
  token,
  SUM(amount) AS amount,
  SUM(amount_usd) AS amount_usd,
  SUM(CASE WHEN token != 'AURA' THEN amount_usd ELSE 0 END) AS amount_usd_sans_aura,
  SUM(CASE WHEN token != 'ARB' THEN amount_usd ELSE 0 END) AS amount_usd_sans_arb,
  SUM(CASE WHEN token NOT IN ('AURA', 'ARB') THEN amount_usd ELSE 0 END) AS amount_usd_sans_aura_and_arb 
FROM arb_aura_bal
GROUP BY 1, 2, 3