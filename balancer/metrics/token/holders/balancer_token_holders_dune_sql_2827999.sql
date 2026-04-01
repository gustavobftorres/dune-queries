-- part of a query repo
-- query name: Balancer Token Holders (Dune SQL)
-- query link: https://dune.com/queries/2827999


WITH addresses AS (
        SELECT "to" AS adr
        FROM erc20_ethereum.evt_Transfer tr
        WHERE contract_address = 0xba100000625a3754423978a60c9317c58a424e3d
), 
    
    transfers AS (
        SELECT  
            day,
            address, 
            token_address,
            SUM(amount) AS amount
        FROM (
            SELECT  date_trunc('day', evt_block_time) AS day,
                    "to" AS address,
                    tr.contract_address AS token_address,
                    CAST (value as double) AS amount
            FROM erc20_ethereum.evt_Transfer tr
            WHERE contract_address = 0xba100000625a3754423978a60c9317c58a424e3d
    
            UNION ALL
            
            SELECT  date_trunc('day', evt_block_time) AS day,
                    "from" AS address,
                    tr.contract_address AS token_address,
                    - 1 * CAST(value as double) AS amount
            FROM erc20_ethereum.evt_Transfer tr
            WHERE contract_address = 0xba100000625a3754423978a60c9317c58a424e3d
    ) t
   GROUP BY 1, 2, 3
 ),
 
    balances_with_gap_days AS (
        SELECT  
            t.day, 
            address, 
            SUM(amount) OVER (PARTITION BY address ORDER BY t.day) AS balance, 
            LEAD(day, 1, now()) OVER (PARTITION BY address ORDER BY t.day) AS next_day
        FROM transfers t
),
    
    days AS (
        SELECT date_sequence AS day
        FROM unnest(sequence(date('2020-01-01'), date(now()), interval '1' day)) as t(date_sequence)
    )
, 
    
    balance_all_days AS (
        SELECT  d.day,
                address,
                SUM(balance/power(10,0)) AS balance
        FROM balances_with_gap_days b
        INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
        GROUP BY 1, 2
        ORDER BY 1, 2
)

SELECT  CAST(b.day as timestamp) as day, COUNT(DISTINCT address) AS "BAL Holders"
FROM balance_all_days b
WHERE balance > 0
GROUP BY 1
ORDER BY 1 DESC