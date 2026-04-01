-- part of a query repo
-- query name: (query_6675327) vested_shareholders
-- query link: https://dune.com/queries/6675327


WITH vested_shareholders AS (
    SELECT address, label FROM (VALUES
        (0x09c4c9dc17fe0ac408c5dfa784912fb9c0c95b25, 'Shareholder (0x09C4)'),
        (0x0a29500ccc6af0b11c72d4e171d925eb0bb7ee15, 'Shareholder (0x0A29)'),
        (0x0b177b7f10faeadd6eee6d2cc46d783f460566c8, 'Shareholder (0x0B17)'),
        (0x0ed9a1323db9653f4650de782ade8504617ecbd3, 'Shareholder (0x0ED9)'),
        (0x19ae63358648795aaf29e36733f04fcef683aa69, 'Shareholder (0x19AE)'),
        (0x1c39babd4e0d7bff33bc27c6cc5a4f1d74c9f562, 'Shareholder (0x1C39)'),
        (0x1c8bcb6348c84122e67a50e513a1e183c0e6929a, 'Shareholder (0x1C8B)'),
        (0x1cbad69d9cc22962a0a885921518c06ed2f04ffd, 'Shareholder (0x1CBA)'),
        (0x22400f33726f0c62ecbac8ee0ba47c117ce5d429, 'Shareholder (0x2240)'),
        (0x242d7cd78cce454946f35f0a263b54fbe228852c, 'Shareholder (0x242D)'),
        (0x28af060e80e2c2105f6f834bfc46f4ce78f1f006, 'Shareholder (0x28AF)'),
        (0x3c221e16a342a5ec114f7259a37ef42b0597c251, 'Shareholder (0x3C22)'),
        (0x4281e53938c3b1c1d3e8afd21c02ce8512cdbc93, 'Shareholder (0x4281)'),
        (0x438fd34eab0e80814a231a983d8bfaf507ae16d4, 'Shareholder (0x438F)'),
        (0x54c3c925b9d715af541b77f9817544bdc663345e, 'Shareholder (0x54C3)'),
        (0x55031f623152cfb63c60a152238b9b3b28c568b0, 'Shareholder (0x5503)'),
        (0x5a1c53d17d9e5d81fa2b985147c78b3fdffdf51b, 'Shareholder (0x5A1C)'),
        (0x5d76a92b7cb9e1a81b8eb8c16468f1155b2f64f4, 'Shareholder (0x5D76)'),
        (0x66c9e1e4fe518cebfe59c9de16e1c780ef5bacd3, 'Shareholder (0x66C9)'),
        (0x6724f3fbb16f542401bfc42c464ce91b6c31001e, 'Shareholder (0x6724)'),
        (0x6aee9dc09702dffab334f3f8e6f3f97c0e7261f4, 'Shareholder (0x6AEE)'),
        (0x6c1050226e3bd757e950141d3052c029c92c5768, 'Shareholder (0x6C10)'),
        (0x6d5dda04760f0515dc131ff4df76a5188ffcdfcb, 'Shareholder (0x6D5D)'),
        (0x6e33b41e44ca2be27e8f65b5231ae61a21044b4a, 'Shareholder (0x6E33)'),
        (0x74c3646adad7e196102d1fe35267adfd401a568b, 'Shareholder (0x74C3)'),
        (0x788c0d59aee802ee615b3db138215862247f5960, 'Shareholder (0x788C)'),
        (0x91b9e59614995e13a32e36440ac524825f7ae39e, 'Shareholder (0x91B9)'),
        (0x942eca417236c4b23b17720716aaa0cc92b0b28f, 'Shareholder (0x942E)'),
        (0x9b71dbccd9ffb858899ef3244b09a5354b16048e, 'Shareholder (0x9B71)'),
        (0xaa857ddce7b5b9cb17296c790cb40e8c11a3d4f0, 'Shareholder (0xAA85)'),
        (0xad3787b9e196804ac0be7cd6bd8a648acd60e1df, 'Shareholder (0xAD37)'),
        (0xaebfe1e1bd01d4b6baf4b998e7b8dc93182d40e0, 'Shareholder (0xAEBF)'),
        (0xb0008192dad242bb58ccbc032587dffbd2096eb9, 'Shareholder (0xB000)'),
        (0xba5c2f2165ddd691f99e12a23ec75cc1519930b4, 'Shareholder (0xBA5C)'),
        (0xbb19053e031d9b2b364351b21a8ed3568b21399b, 'Shareholder (0xBB19)'),
        (0xc75159987ab5bba7df82684aad12af3123a5f667, 'Shareholder (0xC751)'),
        (0xc9cea7a3984cefd7a8d2a0405999cb62e8d206dc, 'Shareholder (0xC9CE)'),
        (0xcaab2680d81df6b3e2ece585bb45cee97bf30cd7, 'Shareholder (0xCAAB)'),
        (0xd09ca75315e70bd3988a47958a0c6c5b30b830e1, 'Shareholder (0xD09C)'),
        (0xd3238d8be92fd856146f53a8b6582bc88e887559, 'Shareholder (0xD323)'),
        (0xd519d5704b41511951c8cf9f65fee9ab9bef2611, 'Shareholder (0xD519)'),
        (0xda6b2a5e0c56542984d84a710f90eefd94ca1991, 'Shareholder (0xDA6B)'),
        (0xdceacfdc8679cc3223541e840a38627e8d2d9fed, 'Shareholder (0xDCEA)'),
        (0xdea0d77a9b02020acf075074881f370a31009982, 'Shareholder (0xDEA0)'),
        (0xdf290293c4a4d6ebe38fd7085d7721041f927e0a, 'Shareholder (0xDF29)'),
        (0xe08a8b19e5722a201eaf20a6bc595ef655397bd5, 'Shareholder (0xE08A)'),
        (0xe7a76d8513e55578c80e4b26fc61ee7d4906d4cd, 'Shareholder (0xE7A7)'),
        (0xf1f7c71cb81ceba1dc5fd659eeb144301df0dbb4, 'Shareholder (0xF1F7)'),
        (0xf3638dad2404be2e95613737a4ad53ac0309c699, 'Shareholder (0xF363)'),
        (0xf96cd1cf416b50b60358a17bc8593060148de422, 'Shareholder (0xF96C)'),
        (0xff052381092420b7f24cc97fded9c0c17b2cbbb9, 'Shareholder (0xFF05)')
    ) AS t(address, label)
)

SELECT 
    address,
    label
FROM vested_shareholders
ORDER BY address