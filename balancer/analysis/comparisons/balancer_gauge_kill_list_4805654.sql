-- part of a query repo
-- query name: Balancer Gauge Kill List
-- query link: https://dune.com/queries/4805654


WITH
    parsed_data AS (
        SELECT 
            CAST(address AS VARCHAR) as gauge, 
            pool_address,
            blockchain,
            name as label,
            status
        FROM labels.balancer_gauges
    ),
    labels AS (
        SELECT 
            gauge, 
            pool_address,
            blockchain,
            label AS symbol,
            status
        FROM parsed_data
    ),
    vote_results AS (
        SELECT
            round_id,
            start_date,
            end_date,
            v.gauge,
            COALESCE(
                l.symbol,
                CONCAT(
                    '0x',
                    LOWER(SUBSTRING(to_hex(v.gauge), 1, 4)),
                    '...',
                    LOWER(SUBSTRING(to_hex(v.gauge), 37, 4))
                )
            ) AS symbol,
            l.pool_address,
            l.blockchain,
            l.status,
            SUM(vote) AS votes
        FROM balancer_ethereum.vebal_votes AS v
        LEFT JOIN labels l ON l.gauge = CAST(v.gauge AS VARCHAR)
        WHERE vote > 0  
        GROUP BY 1, 2, 3, 4, l.symbol, l.status, 6, 7, 8
    ),
    last_active_round AS (
        SELECT
            gauge,
            pool_address,
            blockchain,
            symbol,
            status,
            MAX(round_id) as last_round_id,
            MAX(end_date) as last_vote_date
        FROM vote_results
        GROUP BY gauge, symbol, status, pool_address, blockchain
    ),
    inactive_gauges AS (
        SELECT
            lar.gauge,
            lar.pool_address,
            lar.blockchain,
            lar.symbol,
            lar.status,
            lar.last_round_id,
            lar.last_vote_date,
            DATE_DIFF('day', lar.last_vote_date, CURRENT_DATE) as days_since_last_vote,
            vr.votes as last_vote_amount,
            vr.votes / CAST(tv.total_votes AS DOUBLE) as last_vote_percentage
        FROM last_active_round lar
        LEFT JOIN vote_results vr ON vr.gauge = lar.gauge AND vr.round_id = lar.last_round_id
        LEFT JOIN (
            SELECT round_id, SUM(votes) as total_votes
            FROM vote_results
            GROUP BY round_id
        ) tv ON tv.round_id = lar.last_round_id
        WHERE DATE_DIFF('day', lar.last_vote_date, CURRENT_DATE) > 60
    ),
    liquidity AS (
        SELECT
            day,
            blockchain,
            pool_address,
            SUM(pool_liquidity_usd) AS tvl_usd
        FROM balancer.liquidity
        WHERE day > CURRENT_DATE - INTERVAL '60' day
        GROUP BY 1, 2, 3
    ),
    last_tvl_above_100k AS (
        SELECT 
            blockchain,
            pool_address,
            MAX(day) AS last_day_above_100k
        FROM liquidity
        WHERE tvl_usd > 100000
        GROUP BY 1, 2
    )
    
SELECT
    i.gauge,
    i.symbol,
    i.status,
    i.last_round_id,
    DATE_FORMAT(i.last_vote_date, '%Y-%m-%d') as last_vote_date,
    i.days_since_last_vote,
    ROUND(i.last_vote_amount, 8) as last_vote_amount,
    ROUND(i.last_vote_percentage * 100, 2) as last_vote_percentage,
    APPROX_PERCENTILE(l.tvl_usd, 0.5) AS median_60d_tvl,
    AVG(l.tvl_usd) AS avg_60d_tvl,
    MAX(l.tvl_usd) AS max_60d_tvl,
    MIN(l.tvl_usd) AS min_60d_tvl,
    COUNT(CASE WHEN l.tvl_usd > 100000 THEN l.day END) AS days_above_100k_tvl,
    DATE_FORMAT(lt.last_day_above_100k, '%Y-%m-%d') AS last_day_above_100k
FROM inactive_gauges i
LEFT JOIN liquidity l ON i.pool_address = l.pool_address AND i.blockchain = l.blockchain
LEFT JOIN last_tvl_above_100k lt ON i.pool_address = lt.pool_address AND i.blockchain = lt.blockchain
WHERE i.last_vote_amount > 0  
      AND i.status = 'active'  
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, lt.last_day_above_100k, i.last_vote_amount
ORDER BY i.days_since_last_vote ASC, i.last_vote_amount ASC;
