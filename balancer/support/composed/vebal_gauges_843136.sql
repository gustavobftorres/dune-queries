-- part of a query repo
-- query name: veBAL Gauges
-- query link: https://dune.com/queries/843136


WITH

    parsed_data AS (SELECT * FROM query_2330097), -- Guage Labels
    
    labels AS (SELECT gauge, label AS symbol FROM parsed_data),

    vote_results AS (
        SELECT
            end_date,
            v.gauge,
            symbol,
            SUM(vote) AS votes
        FROM
            --vebal_votes
            query_2265987 AS v
        LEFT JOIN labels l
        ON l.gauge = CAST(v.gauge as VARCHAR)
        WHERE 
            end_date = (
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
        1, 2, 3
    ),
    total_votes AS (
        SELECT end_date, SUM(votes) AS total_votes
        FROM vote_results
        GROUP BY 1
    ),
    ranking as (
        SELECT
            ROW_NUMBER() OVER (ORDER BY votes DESC) AS ranking,
            COALESCE(
                symbol, 
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
                )
            ) AS symbol,
            v.votes / total_votes AS pct_votes,
            v.votes AS votes,
            CONCAT(
                '<a target="_blank" href="https://dune.com/balancerlabs/veBAL-Analysis?2.+Gauge_t0b4cc=0x',
                LOWER(to_hex(gauge)),
                '">view stats</a>'
            ) AS stats,
            gauge
        FROM vote_results v
        LEFT JOIN total_votes t
        ON t.end_date = v.end_date
        ORDER BY 3 DESC NULLS LAST
    )
    
SELECT * FROM ranking
WHERE ('{{2. Gauge}}' = 'All' OR CAST(gauge AS VARCHAR(42)) = '{{2. Gauge}}')