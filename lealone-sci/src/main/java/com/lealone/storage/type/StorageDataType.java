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

    default void write(DataBuffer buff, Object obj, Lockable lockable) {
        write(buff, obj);
    }

    default void writeMeta(DataBuffer buff, Object obj) {
        // do nothing
    }

    default Object readMeta(ByteBuffer buff, int columnCount) {
        // do nothing
        return null;
    }

    default void writeColumn(DataBuffer buff, Object obj, int columnIndex) {
        write(buff, obj);
    }

    default void readColumn(ByteBuffer buff, Object obj, int columnIndex) {
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

    default Object convertToIndexKey(Object key, Object value) {
        return null;
    }

    // 是否只存放key，比如索引不需要value
    default boolean isKeyOnly() {
        return false;
    }

    default boolean isRowOnly() {
        return false;
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
}
