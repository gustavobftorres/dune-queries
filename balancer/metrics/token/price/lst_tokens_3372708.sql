-- part of a query repo
-- query name: LST tokens
-- query link: https://dune.com/queries/3372708


SELECT blockchain, contract_address, symbol FROM tokens.erc20
WHERE blockchain IN ('arbitrum', 'avalanche_c', 'base', 'ethereum', 'gnosis', 'optimism', 'polygon', 'zkevm')
 AND ((symbol LIKE '%ETH' AND symbol != 'WETH')
     OR (symbol LIKE '%MATIC' AND symbol != 'WMATIC' AND blockchain = 'polygon')
     OR (symbol LIKE '%MATIC' AND symbol != 'MATIC' AND blockchain = 'zkevm')
     OR (symbol LIKE '%SOL' AND blockchain = 'arbitrum')
     OR (symbol LIKE '%AVAX' AND symbol != 'WAVAX' AND blockchain = 'avalanche_c'))
 AND symbol NOT LIKE '%-%' AND symbol NOT LIKE '%/%' AND symbol NOT LIKE '%:%'