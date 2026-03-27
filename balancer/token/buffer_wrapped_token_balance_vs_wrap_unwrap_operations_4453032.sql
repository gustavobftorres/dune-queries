-- part of a query repo
-- query name: Buffer Wrapped Token Balance vs. Wrap / Unwrap Operations
-- query link: https://dune.com/queries/4453032


SELECT
    evt_block_time,
    wrapped_balance,
    wrap_unwrap
FROM 
    (SELECT 
    evt_block_time,
    wrapped_balance,
    value AS wrap_unwrap,
    ROW_NUMBER() OVER(ORDER BY evt_block_time ASC) AS rn
    FROM query_4457927 q
    WHERE wrappedToken = {{wrapped_token}}
    AND blockchain = '{{blockchain}}')
WHERE rn > 1