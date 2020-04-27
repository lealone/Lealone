/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

/**
 * The list of setting for a SET statement.
 * 
 * @author H2 Group
 * @author zhh
 */
// 分session和database两种级别的类型
// database级的大多需要保存为Setting
// session级的只要更新当前session的状态即可
// 除了VARIABLE枚举常量使用SET @VARIABLE这样的特殊语法外，其他的都直接在SET后面接枚举常量名
public enum SetType {

    // 以下是session级的类型
    LOCK_TIMEOUT,
    QUERY_TIMEOUT,
    SCHEMA,
    SCHEMA_SEARCH_PATH,
    VARIABLE,
    THROTTLE,

    // 以下是database级的类型
    ALLOW_LITERALS,
    CACHE_SIZE,
    COLLATION,
    BINARY_COLLATION,
    COMPRESS_LOB,
    CREATE_BUILD,
    DATABASE_EVENT_LISTENER,
    DB_CLOSE_DELAY,
    DEFAULT_LOCK_TIMEOUT,
    DEFAULT_TABLE_TYPE,
    EXCLUSIVE,
    IGNORECASE,
    LOCK_MODE,
    MAX_LENGTH_INPLACE_LOB,
    MAX_LOG_SIZE,
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
    WRITE_DELAY;

    public String getName() {
        if (this == VARIABLE)
            return "@";
        else
            return name();
    }

    public static boolean contains(String name) {
        try {
            SetType.valueOf(name);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
