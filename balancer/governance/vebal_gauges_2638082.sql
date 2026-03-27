-- part of a query repo
-- query name: veBAL Gauges
-- query link: https://dune.com/queries/2638082


WITH

    parsed_data AS (SELECT CAST(address AS VARCHAR) as gauge, name as label FROM labels.balancer_gauges), -- Gauge Labels
    
    labels AS (SELECT gauge, label AS symbol FROM parsed_data),

    vote_results AS (
        SELECT
            end_date,
            v.gauge,
            symbol,
            SUM(vote) AS votes
        FROM
            --vebal_votes
            balancer_ethereum.vebal_votes AS v
        LEFT JOIN labels l
        ON l.gauge = CAST(v.gauge as VARCHAR)
        WHERE 
            end_date = (
                SELECT
                    end_date
                FROM
                    --vebal_votes
                    balancer_ethereum.vebal_votes
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
                CONCAT(
                    '<a target="_blank" href="https://etherscan.io/address/0x',
                    LOWER(to_hex(gauge)),
                    '">',
                   symbol,
                    '↗</a>'),
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
                '<a target="_blank" href="https://dune.com/balancer/vebal-gauge-analysis?1.+Gauge_t16ec7=0x2fc4506354166e8b9183fbb6a68cd9c5f3fb9bc5&Gauge_t16ec7=0x',
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
WHERE /*ranking <= 100 AND*/ ('{{Gauge}}' = 'All' OR CAST(gauge AS VARCHAR(42)) = '{{Gauge}}')