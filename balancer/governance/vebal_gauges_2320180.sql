-- part of a query repo
-- query name: veBAL Gauges
-- query link: https://dune.com/queries/2320180


WITH
  vote_results AS (
    SELECT
      end_date,
      gauge,
      SUM(vote) AS votes
    FROM
      --vebal_votes
      query_2265987 AS v
    WHERE end_date = (
            SELECT
              end_date
            FROM
              --vebal_votes
              query_2265987
            WHERE start_date <= CURRENT_DATE
            AND end_date >= CURRENT_DATE
            LIMIT 1
        )
    GROUP BY
      1,
      2
  ),
  total_votes AS (
    SELECT
      end_date,
      SUM(votes) AS total_votes
    FROM
      vote_results
    GROUP BY
    1
  )
    
SELECT
    ROW_NUMBER() OVER (ORDER BY votes DESC) AS ranking,
    CONCAT(
      '<a target="_blank" href="https://etherscan.io/address/0x',
      LOWER(to_hex(gauge)),
      '">',
      CONCAT(
        '0x',
        LOWER(SUBSTRING(to_hex(gauge), 1, 4)),
        '...',
        LOWER(SUBSTRING(to_hex(gauge), 37, 4))
        ),
        '↗</a>'
      ) AS symbol,
    v.votes / total_votes AS pct_votes,
    v.votes AS votes,
    CONCAT(
    '<a href="https://dune.com/balancer/vebal-analysis?2.+Gauge_t93063=0x',
    LOWER(to_hex(gauge)),
    '">view stats</a>'
  ) AS stats
FROM vote_results v
LEFT JOIN total_votes t
ON t.end_date = v.end_date
ORDER BY 3 DESC NULLS LAST
