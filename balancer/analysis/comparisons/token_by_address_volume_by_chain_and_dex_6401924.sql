-- part of a query repo
-- query name: Token (by address) Volume by Chain and DEX
-- query link: https://dune.com/queries/6401924


WITH token_list AS (
    SELECT token_address
    FROM (
        VALUES
            (from_hex('fa2b947eec368f42195f24f36d2af29f7c24cec2')),
            (from_hex('b3b02e4a9fb2bd28cc2ff97b0ab3f6b3ec1ee9d2')),
            (from_hex('8210c0634ab8f273806e4b7866e9db353773c44b')),
            (from_hex('c8cf6d7991f15525488b2a83df53468d682ba4b0'))
    ) AS t(token_address)
)

SELECT
    date_trunc('day', block_time) AS date,
    blockchain,
    project,
    token_pair,
    SUM(amount_usd) AS volume
FROM dex.trades
WHERE block_time >= now() - interval '{{days}}' day
  --AND blockchain = 'base'
  AND amount_usd > 0
  AND (
        token_bought_address IN (SELECT token_address FROM token_list)
        OR token_sold_address  IN (SELECT token_address FROM token_list)
      )
GROUP BY 1, 2, 3, 4
ORDER BY date DESC, volume DESC;
