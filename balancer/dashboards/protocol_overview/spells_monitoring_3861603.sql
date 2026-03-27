-- part of a query repo
-- query name: Spells Monitoring
-- query link: https://dune.com/queries/3861603


SELECT 
    'liquidity' AS spell,
    blockchain,
    MAX(day) AS last_run,
    CASE WHEN
        MAX(day) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer.liquidity
GROUP BY 1, 2

UNION ALL

SELECT 
    'bpt_prices' AS spell,
    blockchain,
    MAX(day) AS last_run,
    CASE WHEN
        MAX(day) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer.bpt_prices
GROUP BY 1, 2

UNION ALL

SELECT 
    'protocol_fee' AS spell,
    blockchain,
    MAX(day) AS last_run,
    CASE WHEN
        MAX(day) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer.protocol_fee
GROUP BY 1, 2

UNION ALL

SELECT 
    'trades' AS spell,
    blockchain,
    MAX(block_date) AS last_run,
    CASE WHEN
        MAX(block_date) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer.trades
GROUP BY 1, 2

UNION ALL

SELECT 
    'bpt_supply' AS spell,
    blockchain,
    MAX(day) AS last_run,
    CASE WHEN
        MAX(day) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer.bpt_supply
GROUP BY 1, 2

UNION ALL

SELECT 
    'pools_metrics_daily' AS spell,
    blockchain,
    MAX(block_date) AS last_run,
    CASE WHEN
        MAX(block_date) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer.pools_metrics_daily
GROUP BY 1, 2

UNION ALL

SELECT 
    'token_balance_changes' AS spell,
    blockchain,
    MAX(block_date) AS last_run,
    CASE WHEN
        MAX(block_date) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer.token_balance_changes
GROUP BY 1, 2

UNION ALL

SELECT 
    'token_balance_changes_daily' AS spell,
    blockchain,
    MAX(block_date) AS last_run,
    CASE WHEN
        MAX(block_date) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer.token_balance_changes_daily
GROUP BY 1, 2

UNION ALL

SELECT 
    'bpt_supply_changes_daily' AS spell,
    blockchain,
    MAX(block_date) AS last_run,
    CASE WHEN
        MAX(block_date) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer.bpt_supply_changes_daily
GROUP BY 1, 2

UNION ALL

SELECT 
    'balancer.bpt_supply_changes' AS spell,
    blockchain,
    MAX(block_date) AS last_run,
    CASE WHEN
        MAX(block_date) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer.bpt_supply_changes
GROUP BY 1, 2

UNION ALL

SELECT 
    'vebal_balances_day' AS spell,
    'ethereum' AS blockchain,
    MAX(day) AS last_run,
    CASE 
    WHEN
        MAX(day) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer_ethereum.vebal_balances_day
GROUP BY 1, 2

UNION ALL

SELECT 
    'vebal_slopes' AS spell,
    'ethereum' AS blockchain,
    MAX(block_date) AS last_run,
    CASE 
    WHEN
        MAX(block_date) < CURRENT_DATE - INTERVAL '7' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer_ethereum.vebal_slopes
GROUP BY 1, 2

UNION ALL

SELECT 
    'vebal_votes' AS spell,
    'ethereum' AS blockchain,
    MAX(start_date) AS last_run,
    CASE 
    WHEN
        MAX(start_date) < CURRENT_DATE - INTERVAL '7' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer_ethereum.vebal_votes
GROUP BY 1, 2

UNION ALL

SELECT 
    'bcowamm_liquidity' AS spell,
    blockchain,
    MAX(day) AS last_run,
    CASE WHEN
        MAX(day) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer_cowswap_amm.liquidity
GROUP BY 1, 2

UNION ALL

SELECT 
    'bcowamm_trades' AS spell,
    blockchain,
    MAX(block_date) AS last_run,
    CASE WHEN
        MAX(block_date) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
    FROM balancer_cowswap_amm.trades
    GROUP BY 1, 2
    
UNION ALL

SELECT 
    'erc4626_token_prices' AS spell,
    blockchain,
    MAX(minute) AS last_run,
    CASE WHEN
        MAX(minute) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status    
FROM balancer_v3.erc4626_token_prices
GROUP BY 1, 2

UNION ALL

SELECT 
    'v3_liquidity' AS spell,
    blockchain,
    MAX(day) AS last_run,
    CASE WHEN
        MAX(day) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
FROM balancer.liquidity
WHERE version = '3'
GROUP BY 1, 2

UNION ALL

SELECT 
    'v3_trades' AS spell,
    blockchain,
    MAX(block_date) AS last_run,
    CASE WHEN
        MAX(block_date) < CURRENT_DATE - INTERVAL '1' day
    THEN 
        'MISSING REFRESH'
    ELSE
        'OK'
    END AS status
    FROM balancer.trades
    WHERE version = '3'
    GROUP BY 1, 2

ORDER BY 4 ASC, 3 ASC, 1 DESC, 2 DESC