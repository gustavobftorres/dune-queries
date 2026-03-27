-- part of a query repo
-- query name: manual_pricing
-- query link: https://dune.com/queries/5169345


with

manual_pricing(token, blockchain, symbol, decimals, price_token, price_blockchain, price_decimals) as (
    values
          (0x00000000efe302beaa2b3e6e1b18d08d69a9012a, 'polygon', 'AUSD', 6, 0xdAC17F958D2ee523a2206206994597C13D831ec7, 'ethereum', 6) -- temp solution: AUSD -> USDT
        , (0xddb46999f8891663a8f2828d25298f70416d7610, 'arbitrum', 'SUSDS', 18, 0x5875eee11cf8398102fdad704c9e96607675467a, 'base', 18) -- no prices for SUSDS on arbitrum
        , (0xa3931d71877c0e7a3148cb7eb4463524fec27fbd, 'ethereum', 'SUSDS', 18, 0x5875eee11cf8398102fdad704c9e96607675467a, 'base', 18) -- missing prices for SUSDS on ethereum
        , (0x57f5e098cad7a3d1eed53991d4d66c45c9af7812, 'polygon', 'USDM', 18, 0x57f5e098cad7a3d1eed53991d4d66c45c9af7812, 'ethereum', 18) -- no prices for USDM on Polygon
        -- , (0x03b54a6e9a984069379fae1a4fc4dbae93b3bccd, 'polygon', 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0, 'ethereum', 18) -- no prices for wstETH on Polygon
        -- , (0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6, 'polygon', 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599, 'ethereum', 8) -- no prices for WBTC on Polygon
        -- , (0x7ceb23fd6bc0add59e62ac25578270cff1b9f619, 'polygon', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 'ethereum', 18) -- no prices for WETH on Polygon

        , (0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f, 'ethereum', 'GHO', 18, 0xdAC17F958D2ee523a2206206994597C13D831ec7, 'ethereum', 6) -- wrong prices for GHO on ethereum
        , (0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf, 'ethereum', 'cbBTC', 8, 0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf, 'base', 8) -- wrong prices for cbBTC on ethereum
        -- , (0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb, 'plasma', 0xdAC17F958D2ee523a2206206994597C13D831ec7, 'ethereum', 6) -- USDT
        -- , (0x9895D81bB462A195b4922ED7De0e3ACD007c32CB, 'plasma', 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2, 'ethereum', 18) -- WETH
        -- , (0x1B64B9025EEbb9A6239575dF9Ea4b9Ac46D4d193, 'plasma', 0x68749665FF8D2d112Fa859AA293F07A622782F38, 'ethereum', 6)  -- XAUT0
        -- , (0x0A1a1A107E45b7Ced86833863f482BC5f4ed82EF, 'plasma', 0x0A1a1A107E45b7Ced86833863f482BC5f4ed82EF, 'arbitrum', 6) -- USDai
        -- , (0x0B2b2B2076d95dda7817e785989fE353fe955ef9, 'plasma', 0x0B2b2B2076d95dda7817e785989fE353fe955ef9, 'arbitrum', 6) -- sUSDai
        -- , (0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34, 'plasma', 0x4c9EDD5852cd905f086C759E8383e09bff1E68B3, 'ethereum', 6) -- USDe
        -- , (0x211Cc4DD073734dA055fbF44a2b4667d5E5fE5d2, 'plasma', 0x9D39A5DE30e57443BfF2A8307A4256c8797A3497, 'ethereum', 6) -- sUSDe
        -- , (0xA3D68b74bF0528fdD07263c60d6488749044914b, 'plasma', 0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee, 'ethereum', 18) -- weETH
        -- , (0xC4374775489CB9C56003BF2C9b12495fC64F0771, 'plasma', 0x356B8d89c1e1239Cbbb9dE4815c39A1474d5BA7D, 'ethereum', 6) -- syrupUSDT
        -- , (0x2a52B289bA68bBd02676640aA9F605700c9e5699, 'plasma', 0x1202F5C7b4B9E47a1A484E8B270be34dbbC75055, 'ethereum', 18) -- wstUSR

        , (from_base58('8smindLdDuySY6i2bStQX9o8DVhALCXCMbNxD98unx35'), 'solana', 'USDCV', 2, 0x5422374b27757da72d5265cc745ea906e0446634, 'ethereum', 18) -- USDCV
        , (from_base58('DghpMkatCiUsofbTmid3M3kAbDTPqDwKiYHnudXeGG52'), 'solana', 'EURCV', 2, 0x5f7827fdeb7c20b443265fc2f40845b715385ff2, 'ethereum', 18) -- EURCV
        , (from_base58('AvZZF1YaZDziPY2RCK4oJrRVrbN3mTD9NL24hPeaZeUj'), 'solana', 'syrupUSDC', 6, 0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b, 'ethereum', 6) -- syrupUSDC
        , (from_base58('JuprjznTrTSp2UFa3ZBUFgwdAmtZCq4MQCwysN55USD'), 'solana', 'jupUSD', 6, from_base58('Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'), 'solana', 6) -- jupUSD
        , (from_base58('cPQPBN7WubB3zyQDpzTK2ormx1BMdAym9xkrYUJsctm'), 'solana', 'fwdSOL', 9, from_base58('So11111111111111111111111111111111111111112'), 'solana', 9)
        , (from_base58('sctmB7GPi5L2Q5G9tUSzXvhZ4YiDMEGcRov9KfArQpx'), 'solana', 'dfdvSOL', 9, from_base58('So11111111111111111111111111111111111111112'), 'solana', 9)
        , (from_base58('98B1NMLYaNJQNxiQGr53vbjNFMNTYFmDqoCgj7qD9Vhm'), 'solana', 'nsJUPITER', 9, from_base58('So11111111111111111111111111111111111111112'), 'solana', 9)
        , (from_base58('2k79y8CApbU9jAvWhLS2j6uRbaVjpLJTUzstBTho9vGq'), 'solana', 'nsHELIUS', 9, from_base58('So11111111111111111111111111111111111111112'), 'solana', 9)
        , (from_base58('9yQLxEzusZ7QiZNafDNdzbEaTCPuJToGjMhLRJtZbgsd'), 'solana', 'nsNANSEN', 9, from_base58('So11111111111111111111111111111111111111112'), 'solana', 9)
        , (from_base58('C1KwBJZNwUaodUcP5kXqD52NCuZzThNAG2cw3vt5H6iE'), 'solana', 'nsSHIFT', 9, from_base58('So11111111111111111111111111111111111111112'), 'solana', 9)
        , (from_base58('PhxXAYTkFZS23ZWvFcz6H6Uq4VnVBMa6hniiAyudjaW'), 'solana', 'nsKILN', 9, from_base58('So11111111111111111111111111111111111111112'), 'solana', 9)
        , (from_base58('38ZUTefZnKSUJU3wxpUe3xpiw2j5WQPnmzSTNbS1JqLA'), 'solana', 'nsTEMPORAL', 9, from_base58('So11111111111111111111111111111111111111112'), 'solana', 9)
        , (from_base58('7GxATsNMnaC88vdwd2t3mwrFuQwwGvmYPrUQ4D6FotXk'), 'solana', 'JUICED', 6, from_base58('7GxATsNMnaC88vdwd2t3mwrFuQwwGvmYPrUQ4D6FotXk'), 'solana', 6) -- rename jlJUPUSD to JUICED
        , (from_base58('5oVNBeEEQvYi1cX3ir8Dx5n1P7pdxydbGF2X4TxVusJm'), 'solana', 'INF', 9, from_base58('5oVNBeEEQvYi1cX3ir8Dx5n1P7pdxydbGF2X4TxVusJm'), 'solana', 9) -- rename SCNSOL to INF


        , (0x8e9d4cea39299323fe8eda678cad449718556c4e, 'bnb', 'syrupUSDT', 6, 0x356B8d89c1e1239Cbbb9dE4815c39A1474d5BA7D, 'ethereum', 6) -- syrupUSDT
        , (0x4254813524695def4163a169e901f3d7a1a55429, 'bnb', 'wstUSR', 18, 0x1202F5C7b4B9E47a1A484E8B270be34dbbC75055, 'ethereum', 18) -- wstUSR
        , (0x66cfbd79257dc5217903a36293120282548e2254, 'arbitrum', 'wstUSR', 18, 0x1202F5C7b4B9E47a1A484E8B270be34dbbC75055, 'ethereum', 18) -- wstUSR
        , (0x2a52b289ba68bbd02676640aa9f605700c9e5699, 'plasma', 'wstUSR', 18, 0x1202F5C7b4B9E47a1A484E8B270be34dbbC75055, 'ethereum', 18) -- wstUSR
        , (0xb67675158b412d53fe6b68946483ba920b135ba1, 'base', 'wstUSR', 18, 0x1202F5C7b4B9E47a1A484E8B270be34dbbC75055, 'ethereum', 18) -- wstUSR
        , (0xf33687811f3ad0cd6b48dd4b39f9f977bd7165a2, 'polygon', 'TruMATIC', 18, 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270, 'polygon', 18) -- TruMATIC


)

select *
from manual_pricing