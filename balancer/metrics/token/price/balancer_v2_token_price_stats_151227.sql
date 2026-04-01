-- part of a query repo
-- query name: Balancer V2 Token Price Stats
-- query link: https://dune.com/queries/151227


/* !Generated view warning: you can't query views in dune_user_generated anymore. All queries in DuneSQL are by default views though (try querying the table 'query_1747157') */
WITH
  lbp_info AS (
    SELECT
      *
    FROM
      query_2511450
    WHERE
      name = '{{LBP}}'
  ),
  pool_token_price AS (
    SELECT
      DATE_TRUNC('hour', block_time) AS hour,
      token_sold_address AS token,
      AVG(
        amount_usd / CAST(
          (
            CAST(token_sold_amount_raw as double)/ POWER(10, COALESCE(decimals, 18))
          ) AS DOUBLE
        )
      ) AS price
    FROM
      dex.trades AS d
      INNER JOIN lbp_info AS l ON l.pool_id = CAST(d.project_contract_address as varchar)
      AND l.token_sold = d.token_sold_address
      LEFT JOIN tokens.erc20 AS t ON t.contract_address = l.token_sold
    WHERE
      t.blockchain = 'ethereum'
      AND d.blockchain = 'ethereum'
      AND project = 'balancer'
      AND block_time <= l.end_time
    GROUP BY
      1,
      2
  ),
  sales_stats AS (
    SELECT
      *
    FROM
      (
        SELECT
          token,
          min_price,
          max_price
        FROM
          (
            SELECT
              token,
              MIN(price) AS min_price,
              MAX(price) AS max_price,
              ROW_NUMBER() OVER (
                PARTITION BY
                  1
              ) AS _row_number
            FROM
              pool_token_price
            GROUP BY
              1
          )
        WHERE
          "_row_number" = 1
      ) AS f
      JOIN (
        SELECT
          token,
          final_price
        FROM
          (
            SELECT
              token,
              price AS final_price,
              ROW_NUMBER() OVER (
                PARTITION BY
                  1
                ORDER BY
                  token,
                  hour DESC
              ) AS _row_number
            FROM
              pool_token_price
          )
        WHERE
          "_row_number" = 1
      ) AS l USING (token)
  )
SELECT
  *
FROM
  sales_stats