/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db;

import java.util.HashSet;

/**
 * The list of database setting for a SET statement.
 */
// database级的大多需要保存为Setting
public enum DbSetting {

    ALIAS_COLUMN_NAME,
    ANALYZE_AUTO,
    ANALYZE_SAMPLE,
    DATABASE_TO_UPPER,
    DB_CLOSE_ON_EXIT,
    DEFAULT_ESCAPE,
    DROP_RESTRICT,
    ESTIMATED_FUNCTION_TABLE_ROWS,
    LARGE_RESULT_BUFFER_SIZE,
    // LARGE_TRANSACTIONS, //暂时用不到
    MAX_QUERY_TIMEOUT,
    OPTIMIZE_DISTINCT,
    OPTIMIZE_EVALUATABLE_SUBQUERIES,
    OPTIMIZE_INSERT_FROM_SELECT,
    OPTIMIZE_IN_LIST,
    OPTIMIZE_IN_SELECT,
    OPTIMIZE_IS_NULL,
    OPTIMIZE_OR,
    OPTIMIZE_TWO_EQUALS,
    QUERY_CACHE_SIZE,
    RECOMPILE_ALWAYS,
    ROWID,
    DEFAULT_STORAGE_ENGINE,
    DEFAULT_SQL_ENGINE,
    DEFAULT_TRANSACTION_ENGINE,
    PERSISTENT,
    CIPHER,
    FILE_PASSWORD_HASH,
    FILE_ENCRYPTION_KEY,

    ALLOW_LITERALS,
    COLLATION,
    BINARY_COLLATION,
    LOB_COMPRESSION_ALGORITHM,
    DATABASE_EVENT_LISTENER,
    DB_CLOSE_DELAY,
    DEFAULT_LOCK_TIMEOUT,
    DEFAULT_TABLE_TYPE,
    EXCLUSIVE,
    IGNORECASE,
    MAX_LENGTH_INPLACE_LOB,
    MAX_MEMORY_ROWS,
    MAX_MEMORY_UNDO,
    MAX_OPERATION_MEMORY,
    MODE,
    OPTIMIZE_REUSE_RESULTS,
    REFERENTIAL_INTEGRITY,
    QUERY_STATISTICS,
    QUERY_STATISTICS_MAX_ENTRIES,
    TRACE_LEVEL_SYSTEM_OUT,
    TRACE_LEVEL_FILE,
    TRACE_MAX_FILE_SIZE,

    // 在存储层也可用的参数
    COMPRESS,
    READ_ONLY,
    PAGE_SIZE,
    CACHE_SIZE;

    public String getName() {
        return name();
    }

    public static boolean contains(String name) {
        try {
            DbSetting.valueOf(name);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    public static HashSet<String> getRecognizedStorageSetting() {
        HashSet<String> set = new HashSet<String>();
        set.add(COMPRESS.name());
        set.add(READ_ONLY.name());
        set.add(PAGE_SIZE.name());
        set.add(CACHE_SIZE.name());
        return set;
    }
}
