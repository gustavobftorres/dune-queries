-- part of a query repo
-- query name: AAVE 80/20 Buy/Sell Volume
-- query link: https://dune.com/queries/4165286


SELECT
    block_month,
    CASE
        WHEN token_bought_symbol = 'AAVE' THEN 'BUY'
        ELSE 'SELL'
    END AS trade_direction,
    SUM(amount_usd) AS volume
FROM dex.trades
WHERE blockchain = 'ethereum'
AND project_contract_address = 0x3de27EFa2F1AA663Ae5D458857e731c129069F29
GROUP BY 1, 2
ORDER BY 1, 2
