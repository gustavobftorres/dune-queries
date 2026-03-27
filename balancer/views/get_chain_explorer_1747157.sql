-- part of a query repo
-- query name: get_chain_explorer
-- query link: https://dune.com/queries/1747157


SELECT 
    *
FROM (
    values 
    ('ethereum', 'https://etherscan.io'),
    ('optimism', 'https://optimistic.etherscan.io'),
    ('polygon', 'https://polygonscan.com'),
    ('arbitrum', 'https://arbiscan.io'),
    ('avalanche_c', 'https://snowtrace.dev'),
    ('gnosis', 'https://gnosisscan.io'),
    ('bnb', 'https://bscscan.com'),
    ('base', 'https://basescan.org'),
    ('celo', 'https://celoscan.io'),
    ('bnb', 'https://bscscan.com'),
    ('goerli', 'https://goerli.basescan.org/'),
    ('solana', 'https://solscan.io')
) as t (chain, explorer)