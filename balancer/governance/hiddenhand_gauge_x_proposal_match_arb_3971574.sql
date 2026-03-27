-- part of a query repo
-- query name: HiddenHand gauge x proposal match (ARB)
-- query link: https://dune.com/queries/3971574


WITH response AS (
    SELECT http_get('https://api.hiddenhand.finance/proposal/aura/1722888000') AS json_response
),
parsed_response AS (
    SELECT
        json_parse(json_response) AS json_parsed
    FROM response
),
extracted_data AS (
    SELECT
        json_extract(json_parsed, '$.data') AS data_array
    FROM parsed_response
),
unnested_data AS (
    SELECT
        json_array_get(data_array, i) AS json_element
    FROM extracted_data,
    UNNEST(SEQUENCE(0, json_array_length(data_array) - 1)) AS t(i)
),
bribes_data AS (
    SELECT
        json_element,
        json_array_get(json_extract(json_element, '$.bribes'), j) AS bribe_element
    FROM unnested_data,
    UNNEST(SEQUENCE(0, json_array_length(json_extract(json_element, '$.bribes')) - 1)) AS t(j)
),

bribes AS(
SELECT
    json_extract_scalar(json_element, '$.proposal') AS proposal,
    json_extract_scalar(json_element, '$.proposalHash') AS proposalHash,
    json_extract_scalar(json_element, '$.title') AS title,
    from_unixtime(cast(json_extract_scalar(json_element, '$.proposalDeadline') AS bigint)) AS proposalDeadline,
    cast(json_extract_scalar(json_element, '$.totalValue') AS double) AS totalValue,
    cast(json_extract_scalar(json_element, '$.maxTotalValue') AS double) AS maxTotalValue,
    cast(json_extract_scalar(json_element, '$.voteCount') AS double) AS voteCount,
    cast(json_extract_scalar(json_element, '$.valuePerVote') AS double) AS valuePerVote,
    cast(json_extract_scalar(json_element, '$.maxValuePerVote') AS double) AS maxValuePerVote,
    json_extract_scalar(json_element, '$.poolId') AS poolId,
    -- Extract fields from the bribe element
    cast(json_extract_scalar(bribe_element, '$.amount') AS double) AS bribe_amount,
    json_extract_scalar(bribe_element, '$.briber') AS briber,
    cast(json_extract_scalar(bribe_element, '$.chainId') AS integer) AS chainId,
    cast(json_extract_scalar(bribe_element, '$.decimals') AS integer) AS decimals,
    cast(json_extract_scalar(bribe_element, '$.maxTokensPerVote') AS double) AS maxTokensPerVote,
    cast(json_extract_scalar(bribe_element, '$.maxValue') AS double) AS maxValue,
    cast(json_extract_scalar(bribe_element, '$.periodIndex') AS integer) AS periodIndex,
    json_extract_scalar(bribe_element, '$.symbol') AS symbol,
    json_extract_scalar(bribe_element, '$.token') AS token,
    cast(json_extract_scalar(bribe_element, '$.value') AS double) AS value
FROM bribes_data)

SELECT DISTINCT proposal, proposalHash FROM bribes 

