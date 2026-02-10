-- Copyright Lealone Database Group.
-- Licensed under the Server Side Public License, v 1.
-- Initial Developer: zhh

create config lealone (
    base_dir: 'target/test-data',
    listen_address: '127.0.0.1',
    scheduler: (
        scheduler_count: 8,
        prefer_batch_write: false,
        max_packet_count_per_loop: 10, -- 每次循环最多读取多少个数据包，默认20
    ),
    storage_engine: (
        name: 'AOSE',
        enabled: true,
        page_size: '16k',
        cache_size: '32m', -- 每个 btree 的缓存大小
        compress: 'no', -- no、lzf、deflate 或 0、1、2
    ),
    transaction_engine: (
        name: 'AOTE',
        enabled: true ,
        redo_log_dir: 'redo_log', -- 会组合成这样的目录: ${base_dir} + "/"+ ${redo_log_dir},
        log_sync_type: 'periodic' -- 取值：instant,no_sync,periodic，默认是periodic
    ),
    sql_engine: (
        name: 'Lealone',
        enabled: true,
    ),
    protocol_server_engine: (
        name: 'TCP',
        enabled: true,
        port: 9210,
        allow_others: true,
        -- white_list: '127.0.0.4,127.0.0.2',
        ssl: false,
        session_timeout: -1
    )
)
