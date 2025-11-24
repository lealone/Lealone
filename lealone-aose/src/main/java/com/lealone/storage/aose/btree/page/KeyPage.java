/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

import com.lealone.db.DataBuffer;
import com.lealone.storage.FormatVersion;
import com.lealone.storage.aose.btree.BTreeMap;

//只有key的场景，比如用来存索引的数据，索引的key就是由索引字段和rowKey组成
public class KeyPage extends RowStorageLeafPage {

    // 内存占用32+48+56=136字节
    public static final int PAGE_MEMORY = 136;

    public KeyPage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    protected int getPageType() {
        return 0;
    }

    @Override
    protected int getEmptyPageMemory() {
        return PAGE_MEMORY;
    }

    @Override
    protected Object[] getValues() {
        return keys;
    }

    @Override
    protected void readValues(ByteBuffer buff, int keyLength, int formatVersion) {
        if (FormatVersion.isOldFormatVersion(formatVersion)) {
            // 兼容老版本
            for (int i = 0; i < keyLength; i++) {
                buff.get();
                buff.get();
                buff.get();
            }
        }
        setPageListener(map.getValueType(), keys);
    }

    @Override
    protected void writeValues(DataBuffer buff, int keyLength, int formatVersion) {
        if (FormatVersion.isOldFormatVersion(formatVersion)) {
            // 兼容老版本
            for (int i = 0; i < keyLength; i++) {
                buff.put((byte) 0);
                buff.put((byte) 0);
                buff.put((byte) 0);
            }
        }
    }

    @Override
    public Object getSplitKey(int index) {
        return map.getKeyType().getSplitKey(getKey(index));
    }
}
