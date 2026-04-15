-- part of a query repo
-- query name: crashed_tokens_broad_v2
-- query link: https://dune.com/queries/6951772



WITH lending_contracts AS (
    SELECT address
    FROM bnb.contracts
    WHERE bytearray_length(code) BETWEEN 3000 AND 50000
      AND (bytearray_position(code, 0x4b8a3529) > 0
        OR bytearray_position(code, 0xc5ebeaec) > 0
        OR bytearray_position(code, 0xf2b9fdb8) > 0
        OR (bytearray_position(code, 0x47bd3718) > 0 
            AND bytearray_position(code, 0x3b1d21a2) > 0))
),
token_recipients AS (
    SELECT 
        CAST(l.contract_address AS VARCHAR) as token,
        CAST(CONCAT('0x', SUBSTR(CAST(l.topic2 AS VARCHAR), 27)) AS VARCHAR) as recipient,
        COUNT(*) as cnt
    FROM bnb.logs l
    WHERE l.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
      AND l.contract_address IN (
        0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82,
        0xcf6bb5389c92bdda8a3747ddb454cb7a64626c63,
        0x8f0528ce5ef7b51152a59745befdd91d97091d2f,
        0xa184088a740c695e156f91f5cc086a06bb78b827,
        0x947950bcc74888a40ffa2593c5798f11fc9124c4,
        0x603c7f932ed1fc6575303d8fb018fdcbb0f39a95,
        0x965f527d9159dce6288a2219db51fc6eef120dd1,
        0x4bd17003473389a42daf6a0a729f6fdb328bbbd7
      )
    GROUP BY 1, 2
    HAVING COUNT(*) >= 1
)
SELECT 
    tr.token,
    tr.recipient as address,
    tr.cnt
FROM token_recipients tr
JOIN lending_contracts lc ON LOWER(CAST(lc.address AS VARCHAR)) = LOWER(tr.recipient)
ORDER BY tr.cnt DESC
LIMIT 500
