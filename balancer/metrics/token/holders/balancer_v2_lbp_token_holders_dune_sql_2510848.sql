-- part of a query repo
-- query name: Balancer V2 LBP Token Holders (Dune SQL)
-- query link: https://dune.com/queries/2510848


WITH lbp_info AS (
        SELECT *
        FROM query_2511450
        WHERE name = '{{LBP}}'
    ),
    
    addresses AS (
        SELECT "to" AS adr
        FROM evms.erc20_transfers tr
        INNER JOIN lbp_info l ON l.token_sold = tr.contract_address AND tr.blockchain = l.blockchain
    ), 
    
    transfers AS (
        SELECT  
            day,
            address, 
            token_address,
            SUM(amount) AS amount
        FROM (
            SELECT  date_trunc('hour', evt_block_time) AS day,
                    "to" AS address,
                    tr.contract_address AS token_address,
                    CAST(value as double) AS amount
            FROM erc20_ethereum.evt_Transfer tr
            INNER JOIN lbp_info l ON l.token_sold = tr.contract_address
    
            UNION ALL
            
            SELECT  date_trunc('hour', evt_block_time) AS day,
                    "from" AS address,
                    tr.contract_address AS token_address,
                    -1 * CAST(value as double) AS amount
            FROM erc20_ethereum.evt_Transfer tr
            INNER JOIN lbp_info l ON l.token_sold = tr.contract_address
        ) t
        GROUP BY 1, 2, 3
    ),
 
    balances_with_gap_days AS (
        SELECT  
            t.day, 
            address, 
            token_address,
            SUM(amount) OVER (PARTITION BY address ORDER BY t.day) AS balance, 
            LEAD(day, 1, now()) OVER (PARTITION BY address ORDER BY t.day) AS next_day
        FROM transfers t
    ),
    
    days AS (
        SELECT date_sequence AS day
        FROM unnest(sequence(date('2020-01-01'), date(now()), interval '1' day)) as t(date_sequence)
), 
    
    balance_all_days AS (
        SELECT  d.day,
                address,
                token_address,
                SUM(balance) AS balance
        FROM balances_with_gap_days b
        INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
        GROUP BY 1, 2, 3
        ORDER BY 1, 2
    )

SELECT  CAST(b.day as timestamp) as day, l.token_symbol, COUNT(DISTINCT address) AS holders
FROM balance_all_days b
INNER JOIN lbp_info l ON l.token_sold = b.token_address
WHERE balance > 0 AND b.day BETWEEN l.start_time AND l.end_time
GROUP BY 1, 2