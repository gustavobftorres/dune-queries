-- part of a query repo
-- query name: veBAL Votes by Gauge Table
-- query link: https://dune.com/queries/2283263


WITH
  vote_results AS (
    SELECT
      round_id,
      start_date,
      end_date,
      v.gauge,
      SUM(vote) AS votes
    FROM
      balancer_ethereum.vebal_votes AS v
    GROUP BY
      1,
      2,
      3,
      4
  ),
  total_votes AS (
    SELECT
      round_id,
      SUM(votes) AS total_votes
    FROM
      vote_results
    GROUP BY
      1
  )
SELECT
  v.round_id,
  DATE_FORMAT(v.start_date, '%Y-%m-%d') AS start_date,
  DATE_FORMAT(v.end_date, '%Y-%m-%d') AS end_date,
  --SUBSTRING(v.start_date, 0, 11) AS start_date,
  --SUBSTRING(v.end_date, 0, 11) AS end_date,
  CONCAT(
    '0x',
    LOWER(SUBSTRING(to_hex(gauge), 1, 4)),
    '...',
    LOWER(SUBSTRING(to_hex(gauge), 37, 4))
  ) AS symbol,
  votes,
  votes / total_votes AS pct_votes
FROM
  vote_results AS v
  JOIN total_votes AS t ON t.round_id = v.round_id
ORDER BY
  1 DESC,
  5 DESC