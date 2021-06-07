/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.session;

/**
 * The list of session setting for a SET statement.
 */
// session级的只要更新当前session的状态即可
// 除了VARIABLE枚举常量使用SET @VARIABLE这样的特殊语法外，其他的都直接在SET后面接枚举常量名
public enum SessionSetting {

    LOCK_TIMEOUT,
    QUERY_TIMEOUT,
    SCHEMA,
    SCHEMA_SEARCH_PATH,
    VARIABLE,
    THROTTLE,
    TRANSACTION_ISOLATION_LEVEL;

    public String getName() {
        if (this == VARIABLE)
            return "@";
        else
            return name();
    }

    public static boolean contains(String name) {
        try {
            SessionSetting.valueOf(name);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
