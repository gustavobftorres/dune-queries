-- part of a query repo
-- query name: vlAURA voters by round
-- query link: https://dune.com/queries/4046652


SELECT
    q.vlaura_round,
    q.vebal_round,
    FROM_UNIXTIME(p.start)- INTERVAL '2' HOUR AS start_date,  
    FROM_UNIXTIME(p."end")- INTERVAL '2' HOUR AS end_date,
    v.voter,
    vp AS voting_power
FROM dune.shot.dataset_votes_view v
LEFT JOIN dune.shot.dataset_proposals_view p
ON v.proposal = p.id
LEFT JOIN query_4001808 q ON (FROM_UNIXTIME(p.start)- INTERVAL '2' HOUR = q.start_Date
OR FROM_UNIXTIME(p.start)- INTERVAL '2' HOUR + interval '7' day = q.start_Date)
WHERE v.space = 'gauges.aurafinance.eth'
ORDER BY 1 DESC, 6 DESC