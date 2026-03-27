-- part of a query repo
-- query name: veBAL Metrics
-- query link: https://dune.com/queries/543807


WITH bal_prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            approx_percentile(price, 0.5) AS price
        FROM prices.usd
        WHERE blockchain = 'ethereum'
        AND contract_address = 0xba100000625a3754423978a60c9317c58a424e3D
        GROUP BY 1
    ),
    
	total_balances AS (
		SELECT
			day,
			sum(total) OVER (ORDER BY day) AS total
		FROM (
			SELECT
				date_trunc('day', evt_block_time) AS day,
				SUM(value / 1e18) AS total
			FROM erc20_ethereum.evt_Transfer
			WHERE CAST(contract_address AS VARCHAR(42)) = '0x5c6ee304399dbdb9c8ef030ab642b10820db8f56'
			AND CAST("from" AS VARCHAR(42)) = '0x0000000000000000000000000000000000000000'
			GROUP BY 1
			UNION ALL
			SELECT
				date_trunc('day', evt_block_time) as day,
				-sum(value / 1e18) AS total
			FROM erc20_ethereum.evt_Transfer
			WHERE CAST(contract_address AS VARCHAR(42)) = '0x5c6ee304399dbdb9c8ef030ab642b10820db8f56'
			AND CAST("to" AS VARCHAR(42)) = '0x0000000000000000000000000000000000000000'
			GROUP BY 1
		) foo
	),
	locked_balances AS (
		SELECT
			day,
			SUM(bpt_balance) AS locked,
			0 AS total
		FROM
        balancer_ethereum.vebal_balances_day
        WHERE day <= CURRENT_DATE
		GROUP BY 1
	)

SELECT
	t.day,
	l.locked,
	t.total,
	l.locked / t.total AS locked_pct,
	l.locked / t.total * 100 AS locked_pct_2,
	l.locked / t.total * token_balance AS bal_locked,
	l.locked / t.total * token_balance * price AS bal_locked_usd
FROM total_balances t
JOIN locked_balances l
ON t.day = l.day
JOIN balancer.liquidity b
ON t.day = b.day
AND pool_id = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014
AND token_address = 0xba100000625a3754423978a60c9317c58a424e3D
JOIN bal_prices p
ON t.day = p.day
ORDER BY 1 DESC