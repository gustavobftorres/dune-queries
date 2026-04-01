-- part of a query repo
-- query name: veBAL voters by round
-- query link: https://dune.com/queries/4047210


WITH vebal_votes AS(
SELECT 
    v.start_date,
    round_id,
    provider,
    SUM(vote) AS total_votes
FROM balancer_ethereum.vebal_votes v
LEFT JOIN balancer_ethereum.vebal_balances_day b
ON v.end_date = b.day
AND b.wallet_address = v.provider
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC),

aura_votes AS(
SELECT
    round_id,
    SUM(vote) AS aura_vebal
FROM balancer_ethereum.vebal_votes
WHERE provider = 0xaf52695e1bb01a16d33d7194c28c42b10e0dbec2
GROUP BY 1
),

vlaura_votes AS(
SELECT
    q.start_date,
    q.vebal_round,
    q.voter AS provider,
    SUM(a.voting_power * b.aura_vebal) AS total_votes
FROM query_4046652 q
LEFT JOIN dune.balancer.result_vl_aura_balances_round a
ON a.vebal_round = q.vebal_round 
AND q.voter = a.delegate
LEFT JOIN aura_votes b ON b.round_id = q.vebal_round
WHERE q.voting_power IS NOT NULL
GROUP BY 1, 2, 3
),

final AS(
SELECT *,
'veBAL' AS source_of_vote
FROM vebal_votes
WHERE provider != 0xaf52695e1bb01a16d33d7194c28c42b10e0dbec2

UNION ALL

SELECT *,
'vlAURA' AS source_of_vote
FROM vlaura_votes 
WHERE total_votes IS NOT NULL)

SELECT 
round_id,
source_of_vote,
provider,
SUM(total_votes) AS total_votes
FROM final
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC