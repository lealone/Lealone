/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.storage.aose.btree;

import java.nio.ByteBuffer;

import org.lealone.db.DataBuffer;
import org.lealone.storage.type.StorageDataType;

class BTreeColumnPage extends BTreePage {

    Object[] values; // 每个元素指向一条记录，并不是字段值
    private int columnIndex;
    private ByteBuffer buff;

    BTreeColumnPage(BTreeMap<?, ?> map) {
        super(map);
    }

    BTreeColumnPage(BTreeMap<?, ?> map, Object[] values, int columnIndex) {
        super(map);
        this.values = values;
        this.columnIndex = columnIndex;
    }

    @Override
    public int getMemory() {
        int memory = 0;
        // 延迟计算
        if (values != null) {
            StorageDataType valueType = map.getValueType();
            for (int row = 0, rowCount = values.length; row < rowCount; row++) {
                memory += valueType.getMemory(values[row], columnIndex);
            }
        }
        return memory;
    }

    protected void recalculateMemory() {
        getMemory();
    }

    @Override
    void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength, boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);

        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);
        buff.get(); // page type;
        int compressType = buff.get(); // page type;

        // 解压完之后就结束了，因为还不知道具体的行，所以延迟对列进行反序列化
        this.buff = expandPage(buff, compressType, start, pageLength);
    }

    // 在read方法中已经把buff读出来了，这里只是把字段从buff中解析出来
    void readColumn(Object[] values, int columnIndex) {
        this.values = values;
        this.columnIndex = columnIndex;
        StorageDataType valueType = map.getValueType();
        for (int row = 0, rowCount = values.length; row < rowCount; row++) {
            valueType.readColumn(buff, values[row], columnIndex);
        }
        buff = null;
    }

    long write(BTreeChunk chunk, DataBuffer buff, boolean replicatePage) {
        int start = buff.position();
        int type = PageUtils.PAGE_TYPE_COLUMN;
        buff.putInt(0); // 回填pageLength

        StorageDataType valueType = map.getValueType();
        int checkPos = buff.position();
        buff.putShort((short) 0);
        buff.put((byte) type);
        int compressTypePos = buff.position();
        int compressType = 0;
        buff.put((byte) compressType);
        int compressStart = buff.position();
        for (int row = 0, rowCount = values.length; row < rowCount; row++) {
            valueType.writeColumn(buff, values[row], columnIndex);
        }
        compressPage(buff, compressStart, compressType, compressTypePos);
        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);
        int chunkId = chunk.id;

        writeCheckValue(buff, chunkId, start, pageLength, checkPos);

        if (!replicatePage) {
            updateChunkAndCachePage(chunk, start, pageLength, type);
        }
        return pos;
    }
}
