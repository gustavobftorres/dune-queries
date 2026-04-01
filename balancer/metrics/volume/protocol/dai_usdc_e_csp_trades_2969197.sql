-- part of a query repo
-- query name: DAI/USDC.e CSP trades
-- query link: https://dune.com/queries/2969197


SELECT block_time, tx_hash, tx_from, tx_to, project_contract_address, token_bought_address, token_sold_address,
CASE WHEN token_bought_address = 0xb86fb1047a955c0186c77ff6263819b37b32440d THEN 'wUSD+'
WHEN token_bought_address = 0x1b224a294920c8c7c534115d3dcb02dc9fb7c0a6 THEN 'wDAI+'
ELSE token_bought_symbol
END AS token_bought_symbol
,
CASE WHEN token_sold_address = 0xb86fb1047a955c0186c77ff6263819b37b32440d THEN 'wUSD+'
WHEN token_sold_address = 0x1b224a294920c8c7c534115d3dcb02dc9fb7c0a6 THEN 'wDAI+'
ELSE token_sold_symbol 
END AS token_sold_symbol,
token_bought_amount,
token_sold_amount,
swap_fee,
amount_usd,
amount_usd * swap_fee as fees_paid
FROM balancer_v2_arbitrum.trades
WHERE ---project_contract_address = 0x519cce718fcd11ac09194cff4517f12d263be067 CSP
project_contract_address IN (0x117a3d474976274b37b7b94af5dcade5c90c6e85,0x284eb68520c8fa83361c1a3a5910aec7f873c18b) --undelying pools
ORDER BY 1 DESC