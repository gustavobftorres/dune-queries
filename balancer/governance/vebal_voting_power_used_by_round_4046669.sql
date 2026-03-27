-- part of a query repo
-- query name: veBAL voting power used by round
-- query link: https://dune.com/queries/4046669


/*
This query calculates the percentage of voting power utilized in each voting round, considering both veBAL 
and vlAURA votes. 
It begins by summing the veBAL balances at the start of each round and comparing these balances with the total 
votes cast.

Since AURA votes now account for approximately 70% of the total veBAL balances, and are determined by vlAURA votes,
the query incorporates these votes as well. 
To do so, the query separates standard veBAL votes from AURA votes.

AURA votes are further analyzed based on those cast via vlAURA delegates and their respective voting power, 
which is scaled according to their delegation and voting power, based on their vlAURA holdings. 
*/


WITH vebal_balances AS(
SELECT 
    day, 
    vebal_round,
    sum(vebal_balance) AS vebal_balance
FROM query_601405 a
INNER JOIN query_4001808 b
ON a.day = start_date
GROUP BY 1, 2
)

SELECT DISTINCT
    vebal_round,
    day,
    b.vebal_balance,
    SUM(t.total_votes),
    SUM(t.total_votes) / b.vebal_balance AS voting_power_used
FROM vebal_balances b
INNER JOIN query_4047210 t
ON b.vebal_round = t.round_id
WHERE b.day < CURRENT_DATE - INTERVAL '8' day
GROUP BY 1, 2, 3
ORDER BY 1 DESC