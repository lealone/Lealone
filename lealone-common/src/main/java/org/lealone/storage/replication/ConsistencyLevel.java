/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.replication;

public enum ConsistencyLevel {
    ALL(0),
    QUORUM(1),
    LOCAL_QUORUM(2, true),
    EACH_QUORUM(3);

    public final int code;
    private final boolean isDCLocal;

    private ConsistencyLevel(int code) {
        this(code, false);
    }

    private ConsistencyLevel(int code, boolean isDCLocal) {
        this.code = code;
        this.isDCLocal = isDCLocal;
    }

    public boolean isDatacenterLocal() {
        return isDCLocal;
    }

    public static ConsistencyLevel getLevel(String code) {
        return getLevel(Integer.parseInt(code));
    }

    public static ConsistencyLevel getLevel(int code) {
        switch (code) {
        case 0:
            return ConsistencyLevel.ALL;
        case 1:
            return ConsistencyLevel.QUORUM;
        case 2:
            return ConsistencyLevel.LOCAL_QUORUM;
        case 3:
            return ConsistencyLevel.EACH_QUORUM;
        default:
            return ConsistencyLevel.ALL;
        }
    }
}
