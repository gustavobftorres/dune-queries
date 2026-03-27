-- part of a query repo
-- query name: PENDLE/ETH (ARB) BPTs Balances
-- query link: https://dune.com/queries/4102799


WITH transfers AS (
    SELECT 
        "from" AS wallet_address,
        DATE_TRUNC('day', evt_block_time) AS block_date,
        -value AS amount,
        CASE 
            WHEN contract_address = 0xf061ac79ca5e21d094a0e02ef3df1ee9ab6f0e0e THEN 'wallet_balance'
            WHEN contract_address = 0xa1231e274c2e4e817923c0a0edc9c5e0d4cb8b80 THEN 'gauge_balance'
        END AS balance_type
    FROM erc20_arbitrum.evt_transfer
    WHERE contract_address IN (
        0xf061ac79ca5e21d094a0e02ef3df1ee9ab6f0e0e,
        0xa1231e274c2e4e817923c0a0edc9c5e0d4cb8b80
    )
    AND DATE_TRUNC('day', evt_block_time) <= TIMESTAMP '{{end date}}'
    AND DATE_TRUNC('day', evt_block_time) >= TIMESTAMP '{{start date}}'

    UNION ALL

    SELECT 
        to AS wallet_address,
        DATE_TRUNC('day', evt_block_time) AS block_date,
        value AS amount,
        CASE 
            WHEN contract_address = 0xf061ac79ca5e21d094a0e02ef3df1ee9ab6f0e0e THEN 'wallet_balance'
            WHEN contract_address = 0xa1231e274c2e4e817923c0a0edc9c5e0d4cb8b80 THEN 'gauge_balance'
        END AS balance_type
    FROM erc20_arbitrum.evt_transfer
    WHERE contract_address IN (
        0xf061ac79ca5e21d094a0e02ef3df1ee9ab6f0e0e,
        0xa1231e274c2e4e817923c0a0edc9c5e0d4cb8b80
    )
    AND DATE_TRUNC('day', evt_block_time) <= TIMESTAMP '{{end date}}'
    AND DATE_TRUNC('day', evt_block_time) >= TIMESTAMP '{{start date}}'
),

balances AS (
    SELECT 
        block_date,
        wallet_address,
        SUM(CASE WHEN balance_type = 'wallet_balance' THEN amount ELSE 0 END) AS wallet_balance,
        SUM(CASE WHEN balance_type = 'gauge_balance' THEN amount ELSE 0 END) AS gauge_balance        
    FROM transfers
    WHERE wallet_address NOT IN (
        0xBA12222222228d8Ba445958a75a0704d566BF2C8, -- Vault
        0xa1231e274c2e4e817923c0a0edc9c5e0d4cb8b80, -- Balancer Gauge
        0x0000000000000000000000000000000000000000  -- Zero Address
    )
    GROUP BY block_date, wallet_address    
),

    calendar AS (
        SELECT date_sequence AS day
        FROM unnest(sequence(date(TIMESTAMP '{{start date}}'), date(now()), interval '1' day)) as t(date_sequence)
    ),

final AS (
    SELECT
        day,
        wallet_address,
        SUM(wallet_balance / 1e18) OVER (PARTITION BY wallet_address ORDER BY day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wallet_balance,
        SUM(gauge_balance / 1e18) OVER (PARTITION BY wallet_address ORDER BY day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS gauge_balance
    FROM calendar c
    JOIN balances b ON c.day = b.block_date
) 

SELECT
    day,
    wallet_address,
    wallet_balance,
    gauge_balance,
    wallet_balance + gauge_balance AS total_balance
FROM final
ORDER BY day DESC, total_balance DESC;