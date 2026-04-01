-- part of a query repo
-- query name: veBAL votes by network
-- query link: https://dune.com/queries/1015861


WITH
	gauge_mapping AS (
		SELECT
            address AS gauge,
            CASE WHEN blockchain = 'polygon'
            THEN 'polygon (PoS)'
            WHEN blockchain = 'zkevm'
            THEN 'polygon (Zk)'
            ELSE blockchain
            END AS gauge_type
        FROM labels.balancer_gauges
	),
	vote_results AS (
		SELECT
			end_date,
            gauge_type,
			SUM(vote) AS votes
		FROM
			--vebal_votes
			balancer_ethereum.vebal_votes AS v
		LEFT JOIN gauge_mapping AS l ON l.gauge = v.gauge
		GROUP BY 1, 2
	),
	total_votes AS (
		SELECT end_date, SUM(votes) AS total_votes
		FROM vote_results
		GROUP BY 1
	)
	
SELECT
	v.end_date,
	v.gauge_type ||
        CASE 
            WHEN v.gauge_type = 'arbitrum' THEN ' 🟦'
            WHEN v.gauge_type = 'avalanche_c' THEN ' ⬜ '
            WHEN v.gauge_type = 'base' THEN ' 🟨'
            WHEN v.gauge_type = 'ethereum' THEN ' Ξ'
            WHEN v.gauge_type = 'gnosis' THEN ' 🟩'
            WHEN v.gauge_type = 'optimism' THEN ' 🔴'
            WHEN v.gauge_type = 'polygon (PoS)' THEN ' 🟪'
            WHEN v.gauge_type = 'polygon (Zk)' THEN ' 🟣'
            WHEN v.gauge_type = 'veBAL (8020)' THEN ' ⚖'
        END 
    AS gauge_type, 
	SUM(v.votes) / total_votes AS pct_votes,
	SUM(v.votes) AS votes
FROM
	vote_results AS v
LEFT JOIN total_votes AS t ON t.end_date = v.end_date
GROUP BY 1, 2, total_votes
ORDER BY 1 DESC, 3 DESC
