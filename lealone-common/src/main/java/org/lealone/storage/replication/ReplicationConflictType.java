/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.replication;

public enum ReplicationConflictType {
    NONE(-1),
    DB_OBJECT_LOCK(0),
    ROW_LOCK(1),
    APPEND(2);

    public final int value;

    private ReplicationConflictType(int value) {
        this.value = value;
    }

    public static ReplicationConflictType getType(int value) {
        switch (value) {
        case 0:
            return ReplicationConflictType.DB_OBJECT_LOCK;
        case 1:
            return ReplicationConflictType.ROW_LOCK;
        case 2:
            return ReplicationConflictType.APPEND;
        default:
            return ReplicationConflictType.NONE;
        }
    }
}
