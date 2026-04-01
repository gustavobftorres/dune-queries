-- part of a query repo
-- query name: Base_potential_arbitrage_priority_fees_txs
-- query link: https://dune.com/queries/4247335


WITH transactions AS (
    SELECT
        block_date,
        block_time,
        block_number,
        tx_hash,
        tx_to,
        TRY(tx_fee_breakdown['priority_fee']) AS priority_fee,
        TRY(tx_fee_breakdown_usd['priority_fee']) AS priority_fee_usd
    FROM
        gas_base.fees
    WHERE 
    tx_to  IN (SELECT DISTINCT address FROM query_3004790
        WHERE blockchain = 'base'
        AND name = 'Arbitrage Bot')
        AND block_time >= TIMESTAMP '{{start_time}}'
        AND block_time <= TIMESTAMP '{{end_time}}'
        
)

SELECT * FROM transactions
WHERE priority_fee IS NOT NULL
ORDER BY priority_fee DESC