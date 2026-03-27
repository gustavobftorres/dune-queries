-- part of a query repo
-- query name: ReCLAMMs Performance
-- query link: https://dune.com/queries/5089450


SELECT DISTINCT
    bal1.day, 
    hodl.current_value_of_investment as "HODL",
    uni.current_value_of_investment as "Uni v2",
    bal1.current_value_of_investment as "ReCLAMM #1",
    bal2.current_value_of_investment as "ReCLAMM #2",
    bal3.current_value_of_investment as "ReCLAMM #3",
    bal4.current_value_of_investment as "ReCLAMM #4",
    bal5.current_value_of_investment as "ReCLAMM #5",
    bal6.current_value_of_investment as "ReCLAMM #6",
    bal7.current_value_of_investment as "ReCLAMM #7"
FROM "query_4771209(start='{{start}}', blockchain='base', pool='0x1A0cde11fD13E9E347088e4cDc00801997911A75')" bal1
LEFT JOIN "query_4771257(start='{{start}}', blockchain='base', token_a='0x4200000000000000000000000000000000000006', token_b='0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913')" hodl
ON bal1.day=hodl.day
LEFT JOIN "query_4771259(start='{{start}}', blockchain='base', pool='0x88A43bbDF9D098eEC7bCEda4e2494615dfD9bB9C', token_a='0x4200000000000000000000000000000000000006', token_b='0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913')" uni
ON bal1.day=uni.day
LEFT JOIN "query_4771209(start='{{start}}', blockchain='base', pool='0xd9a8bd46fbB0BaC27aA1A99E64931d406e3bBb3F')" bal2
ON bal1.day=bal2.day
LEFT JOIN "query_4771209(start='{{start}}', blockchain='base', pool='0x6B54B954E53c3fBaf84B6b97377f3760C91DB847')" bal3
ON bal1.day=bal3.day
LEFT JOIN "query_4771209(start='{{start}}', blockchain='base', pool='0x785D9232cB7195A7ddBA3864f30B750FD7596faC')" bal4
ON bal1.day=bal4.day
LEFT JOIN "query_4771209(start='{{start}}', blockchain='base', pool='0x63B52EBA7e565CcEC991910Bd3482D01bA3Bf70d')" bal5
ON bal1.day=bal5.day
LEFT JOIN "query_4771209(start='{{start}}', blockchain='base', pool='0x7Dc81fb7e93cdde7754bff7f55428226bD9cEF7b')" bal6
ON bal1.day=bal6.day
LEFT JOIN "query_4771209(start='{{start}}', blockchain='base', pool='0xc46e6A1CB1910c916620Dc81C7fd8c38891E1904')" bal7
ON bal1.day=bal7.day
ORDER BY bal1.day DESC
