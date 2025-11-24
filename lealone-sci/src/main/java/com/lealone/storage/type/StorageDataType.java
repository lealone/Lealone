/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.type;

import java.nio.ByteBuffer;

import com.lealone.db.DataBuffer;
import com.lealone.db.lock.Lockable;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueDataType;

public interface StorageDataType extends ValueDataType {

    default void write(DataBuffer buff, Lockable lockable, Object lockedValue, int formatVersion) {
        write(buff, lockedValue, formatVersion);
    }

    default void writeMeta(DataBuffer buff, Object obj, int formatVersion) {
        // do nothing
    }

    default Object readMeta(ByteBuffer buff, Object obj, int columnCount, int formatVersion) {
        // do nothing
        return obj;
    }

    default void writeColumn(DataBuffer buff, Object obj, int columnIndex, int formatVersion) {
        write(buff, obj, formatVersion);
    }

    default void readColumn(ByteBuffer buff, Object obj, int columnIndex, int formatVersion) {
        // do nothing
    }

    default void setColumns(Object oldObj, Object newObj, int[] columnIndexes) {
        // do nothing
    }

    default ValueArray getColumns(Object obj) {
        return null;
    }

    default int getColumnCount() {
        return 1;
    }

    default int getMemory(Object obj, int columnIndex) {
        return getMemory(obj);
    }

    default int getColumnsMemory(Object obj) {
        return 0;
    }

    default Object convertToIndexKey(Object key, Object value) {
        return null;
    }

    default boolean isLockable() {
        return false;
    }

    // 是否只存放key，比如索引不需要value
    default boolean isKeyOnly() {
        return false;
    }

    default boolean isRowOnly() {
        return false;
    }

    default void setRowOnly(boolean rowOnly) {
    }

    default StorageDataType getRawType() {
        return this;
    }

    default Object merge(Object fromObj, Object toObj) {
        return toObj;
    }

    default Object getSplitKey(Object keyObj) {
        return keyObj;
    }

    default Object getAppendKey(long key, Object valueObj) {
        return Long.valueOf(key);
    }

    default int getMetaVersion() {
        return 0;
    }
}
