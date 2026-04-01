-- part of a query repo
-- query name: MIMIC Report
-- query link: https://dune.com/queries/3516357


with 
bal_price as(
    SELECT 
        DATE_TRUNC('day', minute) AS day, 
        APPROX_PERCENTILE(price, 0.5) AS price
    FROM prices.usd
    WHERE blockchain = 'ethereum'
    AND symbol = 'BAL'
    GROUP BY 1
),

bridge_activity as (
    SELECT 
        block_date,
        ROW_NUMBER() OVER(ORDER BY block_date) AS row_num,
        network, 
        amount
    FROM (
        SELECT 
           'gnosis' AS network,
           logs.block_date,
           bytearray_to_uint256(logs.data) as amount
        FROM gnosis.logs logs
        INNER JOIN gnosis.transactions tx ON tx.hash = logs.tx_hash
        WHERE logs.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
        AND logs.block_date >= CAST('2023-09-28' AS TIMESTAMP)
        AND logs.block_date <= CURRENT_DATE
        AND (
           logs.topic1 = bytearray_concat(0x000000000000000000000000, 0x94Dd9C6152a2A0BBcB52d3297b723A6F01D5F9f7) -- Sender sv v2
        )
        AND tx."to" in (
            0x07F51f658d7a9DebF6F7715600DB5784a83735E2  --Hop Bridger Task
        )
        UNION ALL
        SELECT 
           'arbitrum' AS network,
           tx.block_date,
           bytearray_to_uint256(logs.data) as amount
        FROM arbitrum.logs logs
        INNER JOIN arbitrum.transactions tx ON tx.hash = logs.tx_hash
        WHERE logs.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
        AND logs.block_date >= CAST('2023-09-28' AS TIMESTAMP)
        AND logs.block_date <= CURRENT_DATE
        AND (
            logs.topic1 = bytearray_concat(0x000000000000000000000000, 0x94Dd9C6152a2A0BBcB52d3297b723A6F01D5F9f7) -- Sender sv v2
        )
        AND tx."to" in (
            0x8eE0AC6Cc3844C657fa4828ceC7060d171129408  --Hop Bridger Task
        )
        UNION ALL
        SELECT 
           'polygon' AS network,
           tx.block_date,
           bytearray_to_uint256(logs.data) as amount
        FROM polygon.logs logs
        INNER JOIN polygon.transactions tx ON tx.hash = logs.tx_hash
        WHERE logs.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
        AND logs.block_date >= CAST('2023-09-28' AS TIMESTAMP)
        AND logs.block_date <= CURRENT_DATE
        AND (
           logs.topic1 = bytearray_concat(0x000000000000000000000000, 0x94Dd9C6152a2A0BBcB52d3297b723A6F01D5F9f7) -- Sender sv v2
        )
        AND tx."to" in (
            0xF977C9D2326A4d655c4578B4D1534447d173d4c9  --Hop Bridger Task
        )
    ) t
),
bridge_mainnet_v2 AS (
    SELECT 
        tx.block_date,
        ROW_NUMBER() OVER(ORDER BY tx.block_date) AS row_num,
       'ethereum' AS network,
       bytearray_to_uint256(logs.data) as amount
    FROM ethereum.logs logs
    INNER JOIN ethereum.transactions tx ON tx.hash = logs.tx_hash
    WHERE logs.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
    AND logs.block_date >= CAST('2023-09-28' AS TIMESTAMP)
    AND logs.block_date <= CURRENT_DATE
    AND (
        logs.topic2 = bytearray_concat(0x000000000000000000000000, 0x94Dd9C6152a2A0BBcB52d3297b723A6F01D5F9f7) -- Receiver sv v2
    )
    AND tx."to" in (
        0x3CACa7b48D0573D793d3b0279b5F0029180E83b6, --Connext 
        0x4cb69FaE7e7Af841e44E1A1c30Af640739378bb2, --Wormhole 
        0x3666f603Cc164936C1b87e207F36BEBa4AC5f18a  --Hop
    )
),
bridge_avalanche AS (
  SELECT 
       'avalanche' AS network,
       tx.block_date,
       bytearray_to_uint256(logs.data) as amount
    FROM ethereum.logs logs
    INNER JOIN ethereum.transactions tx ON tx.hash = logs.tx_hash
    WHERE logs.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
    AND logs.block_date >= CAST('2023-09-28' AS TIMESTAMP)
    AND logs.block_date <= CURRENT_DATE
    AND logs.topic1 = bytearray_concat(0x000000000000000000000000, 0xDFf6CF2Ea01685bF016Ce31C4CB25ca25B7B8Fdf) -- Sender depositor avax v3
    AND logs.topic2 = bytearray_concat(0x000000000000000000000000, 0x9e5D6427D2cdaDC68870197b099C2Df535Ec3c97) -- Receiver sv v3
),
bridge_base AS (
    SELECT 
       'base' AS network,
       tx.block_date,
       bytearray_to_uint256(logs.data) as amount
    FROM ethereum.logs logs
    INNER JOIN ethereum.transactions tx ON tx.hash = logs.tx_hash
    WHERE logs.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
    AND logs.block_date >= CAST('2023-09-28' AS TIMESTAMP)
    AND logs.block_date <= CURRENT_DATE
    AND logs.topic1 = bytearray_concat(0x000000000000000000000000, 0x85B45B363Ec0023885f86775DdFaf6D879643ED7) -- Sender depositor base v3
    AND logs.topic2 = bytearray_concat(0x000000000000000000000000, 0x9e5D6427D2cdaDC68870197b099C2Df535Ec3c97) -- Receiver sv v3
),
task_withdraw_mainnet AS (
    SELECT 
        tx.block_date,
       'ethereum' AS network,
        tx.hash,
        logs.contract_address as token,
       bytearray_to_uint256(logs.data) as amount
    FROM ethereum.logs logs
    INNER JOIN ethereum.transactions tx ON tx.hash = logs.tx_hash
    WHERE logs.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
    AND logs.block_date >= CAST('2023-09-28' AS TIMESTAMP)
    AND logs.block_date <= CURRENT_DATE
    AND logs.topic1 = bytearray_concat(0x000000000000000000000000, 0x94Dd9C6152a2A0BBcB52d3297b723A6F01D5F9f7) -- Sender sv v2
    AND tx."to" in (
        0x67dCE1ff38d2038C283B8eAd583F478460f5eB58 --Withdrawer v2
    )
    UNION ALL
    SELECT 
        tx.block_date,
       'ethereum' AS network,
        tx.hash,
        logs.contract_address as token,
        bytearray_to_uint256(logs.data) as amount
    FROM ethereum.logs logs
    INNER JOIN ethereum.transactions tx ON tx.hash = logs.tx_hash
    WHERE logs.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
    AND logs.block_date >= CAST('2023-09-28' AS TIMESTAMP)
    AND logs.block_date <= CURRENT_DATE
    AND logs.topic1 = bytearray_concat(0x000000000000000000000000, 0x9e5D6427D2cdaDC68870197b099C2Df535Ec3c97) -- Sender sv v3
    AND tx."to" in (
        0xD7252C026c3cA28D73B4DeeF62FE6ADe86eC17A9 --Relayer v3
    )
    AND logs.tx_hash in (
        SELECT 
            logs.tx_hash
        FROM ethereum.logs logs
        WHERE logs.topic0 = 0x68f46c45a243a0e9065a97649faf9a5afe1692f2679e650c2f853b9cd734cc0e -- Executed event v3
        AND logs.contract_address = 0x3c50C3D7DFd0fc0A988F07479f7f7B39A7FC4cF0 -- Wtidhraw task v3
    )
),
task_withdraw_optimism AS (
    SELECT 
        tx.block_date,
       'optimism' AS network,
        tx.hash,
        logs.contract_address as token,
       bytearray_to_uint256(logs.data) as amount
    FROM optimism.logs logs
    INNER JOIN optimism.transactions tx ON tx.hash = logs.tx_hash
    WHERE logs.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
    AND logs.block_date >= CAST('2023-09-28' AS TIMESTAMP)
    AND logs.block_date <= CURRENT_DATE
    AND logs.topic1 = bytearray_concat(0x000000000000000000000000, 0x94Dd9C6152a2A0BBcB52d3297b723A6F01D5F9f7) -- Sender sv v2
    AND tx."to" in (
        0x2AAb8291b2099Cb11421eD84CC45C87c793A5A77 --Withdrawer v2
    )
),
task_fee_mainnet AS (
    SELECT 
        logs.block_date,
       logs.tx_hash AS hash,
       logs.contract_address as token,
       bytearray_to_uint256(from_hex(SUBSTRING(to_hex(logs.data), 1, 64))) AS amount
    FROM ethereum.logs logs  
    WHERE logs.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
    --AND logs.topic1 = bytearray_concat(0x000000000000000000000000, 0x94Dd9C6152a2A0BBcB52d3297b723A6F01D5F9f7) -- Sender Sv v2
    AND logs.topic2 = bytearray_concat(0x000000000000000000000000, 0x4629c578a9e49ef4aaabfee03f238cb11625f78b) -- Receiver Fee collector
    AND logs.block_date >= CAST('2023-09-28' AS TIMESTAMP)
    AND logs.block_date <= CURRENT_DATE
    AND logs.tx_hash in (
        SELECT hash from task_withdraw_mainnet
    )
),
task_fee_optimism AS (
    SELECT 
       logs.block_date,
       logs.tx_hash AS hash,
       logs.contract_address as token,
       bytearray_to_uint256(from_hex(SUBSTRING(to_hex(logs.data), 1, 64))) AS amount
    FROM optimism.logs logs  
    WHERE logs.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
    --AND logs.topic1 = bytearray_concat(0x000000000000000000000000, 0x94Dd9C6152a2A0BBcB52d3297b723A6F01D5F9f7)
    AND logs.topic2 = bytearray_concat(0x000000000000000000000000, 0x4629c578a9e49ef4aaabfee03f238cb11625f78b) -- Receiver Fee collector
    AND logs.block_date >= CAST('2023-09-28' AS TIMESTAMP)
    AND logs.block_date <= CURRENT_DATE
    AND logs.tx_hash in (
        SELECT hash from task_withdraw_optimism
    )
),

activities AS ( 
SELECT 
    bridge_activity.block_date,
    'bridge' as "type",
    bridge_activity.network,
    'USDC' as token,
    SUM(bridge_mainnet_v2.amount) / POWER(10,6) as amount
FROM bridge_activity
INNER JOIN bridge_mainnet_v2 ON bridge_mainnet_v2.row_num = bridge_activity.row_num
GROUP BY bridge_activity.network, bridge_activity.block_date
UNION ALL
SELECT
    block_date,
    'bridge' as "type",
    network,
    'USDC' as token,
    SUM(amount) / POWER(10,6)
FROM bridge_avalanche
GROUP BY network, block_date
UNION ALL
SELECT
    block_date,
    'bridge' as "type",
    network,
    'USDC' as token,
    SUM(amount) / POWER(10,6)
FROM bridge_base
GROUP BY network, block_date
UNION ALL
SELECT
    block_date,
    'withdraw' as "type",
    network,
    erc20.symbol as token,
    CASE WHEN erc20.symbol IN ('USDC', 'USDC.e')
    THEN SUM(amount) / POWER(10,6)
    WHEN erc20.symbol IN ('BAL')
    THEN SUM(amount * price / POWER(10, 18))    
    ELSE sum(amount)
    END
FROM task_withdraw_mainnet
LEFT JOIN tokens.erc20 erc20 on erc20.contract_address = task_withdraw_mainnet.token
LEFT JOIN bal_price ON block_date = day
GROUP BY network, erc20.symbol, block_date
UNION ALL
SELECT
    block_date,
    'withdraw' as "type",
    network,
    erc20.symbol as token,
    CASE WHEN erc20.symbol IN ('USDC', 'USDC.e')
    THEN SUM(amount) / POWER(10,6)
    WHEN erc20.symbol IN ('BAL')
    THEN SUM(amount * price / POWER(10, 18))    
    ELSE sum(amount)
    END
FROM task_withdraw_optimism
LEFT JOIN tokens.erc20 erc20 on erc20.contract_address = task_withdraw_optimism.token
LEFT JOIN bal_price ON block_date = day
GROUP BY network, erc20.symbol, block_date
UNION ALL
SELECT
    block_date,
    'fee' as "type",
    'ethereum' as network,
    erc20.symbol as token,
    CASE WHEN erc20.symbol IN ('USDC', 'USDC.e')
    THEN SUM(amount) / POWER(10,6)
    WHEN erc20.symbol IN ('BAL')
    THEN SUM(amount * price / POWER(10, 18))    
    ELSE sum(amount)
    END
FROM task_fee_mainnet
LEFT JOIN tokens.erc20 erc20 on erc20.contract_address = task_fee_mainnet.token
LEFT JOIN bal_price ON block_date = day
GROUP BY erc20.symbol, block_date
UNION ALL
SELECT
    block_date,
    'fee' as "type",
    'optimism' as network,
    erc20.symbol as token,
    CASE WHEN erc20.symbol IN ('USDC', 'USDC.e')
    THEN SUM(amount) / POWER(10,6)
    WHEN erc20.symbol IN ('BAL')
    THEN SUM(amount * price / POWER(10, 18))    
    ELSE sum(amount)
    END
FROM task_fee_optimism
LEFT JOIN tokens.erc20 erc20 on erc20.contract_address = task_fee_optimism.token
LEFT JOIN bal_price ON block_date = day
GROUP BY erc20.symbol, block_date
ORDER BY "type"),

dates_cte AS (
    SELECT
        date_sequence AS date
    FROM UNNEST(sequence(date '2023-09-28', CURRENT_DATE, interval '14' day)) AS t(date_sequence)
),

epoch_with_next_change AS (
    SELECT 
        date,
        ROW_NUMBER() OVER(ORDER BY date ASC) AS epoch,
        COALESCE(LEAD(date) OVER(ORDER BY date ASC), CURRENT_DATE) AS next_change_date
    FROM dates_cte
),

daily_epoch AS (
SELECT 
    date AS start_date,
    next_change_date AS end_date,
    date_sequence AS day,
    epoch
FROM epoch_with_next_change
CROSS JOIN UNNEST(sequence(epoch_with_next_change.date, epoch_with_next_change.next_change_date, interval '1' day)) AS t(date_sequence)
)

SELECT
    CONCAT(CAST(e.start_date AS VARCHAR),' - ' ,CAST(e.end_date AS VARCHAR)) AS interval,
    type,
    network,
    SUM(amount) AS amount
FROM daily_epoch e
INNER JOIN activities a ON e.day = a.block_date
--WHERE type = 'withdraw'
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2 ASC, 4 DESC
