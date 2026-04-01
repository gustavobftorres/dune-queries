-- part of a query repo
-- query name: Unnest Array into 1 Column
-- query link: https://dune.com/queries/2511097


with
    pool_id_output AS (
        WITH 
            input AS (SELECT array[1,2,3] AS pool_ids)
            --input AS (SELECT array[{{unnamed_parameter}}] AS pool_ids)
            
        SELECT pool_id_output.pool_ids
        FROM input
        CROSS JOIN UNNEST(input.pool_ids) AS pool_id_output(pool_ids)
    ),
    linear_pool_add as (SELECT substring(CAST(pool_ids AS VARCHAR), 1, 42) AS linear_pool FROM pool_id_output)

select * FROM linear_pool_add