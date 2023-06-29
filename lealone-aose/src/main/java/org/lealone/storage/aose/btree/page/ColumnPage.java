/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

import org.lealone.db.DataBuffer;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.chunk.Chunk;
import org.lealone.storage.type.StorageDataType;

public class ColumnPage extends Page {

    Object[] values; // 每个元素指向一条记录，并不是字段值
    private int columnIndex;

    ColumnPage(BTreeMap<?, ?> map) {
        super(map);
    }

    ColumnPage(BTreeMap<?, ?> map, Object[] values, int columnIndex) {
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

    @Override
    public void read(PageInfo pInfo, ByteBuffer buff, int chunkId, int offset, int expectedPageLength,
            boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);

        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);
        buff.get(); // page type;
        int compressType = buff.get();

        // 解压完之后就结束了，因为还不知道具体的行，所以延迟对列进行反序列化
        pInfo.buff = expandPage(buff, compressType, start, pageLength);
    }

    // 在read方法中已经把buff读出来了，这里只是把字段从buff中解析出来
    void readColumn(PageInfo pInfo, Object[] values, int columnIndex) {
        this.values = values;
        this.columnIndex = columnIndex;
        StorageDataType valueType = map.getValueType();
        for (int row = 0, rowCount = values.length; row < rowCount; row++) {
            valueType.readColumn(pInfo.buff, values[row], columnIndex);
        }
        pInfo.buff.flip(); // 可以复用
    }

    long write(Chunk chunk, DataBuffer buff) {
        beforeWrite();
        PagePos oldPagePos = posRef.get();
        int start = buff.position();
        int type = PageUtils.PAGE_TYPE_COLUMN;
        buff.putInt(0); // 回填pageLength

        StorageDataType valueType = map.getValueType();
        int checkPos = buff.position();
        buff.putShort((short) 0);
        buff.put((byte) type);
        int compressTypePos = buff.position();
        int compressType = 0;
        buff.put((byte) compressType); // 调用compressPage时会回填
        int compressStart = buff.position();
        for (int row = 0, rowCount = values.length; row < rowCount; row++) {
            valueType.writeColumn(buff, values[row], columnIndex);
        }
        compressPage(buff, compressStart, compressType, compressTypePos);
        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);

        writeCheckValue(buff, chunk, start, pageLength, checkPos);
        return updateChunkAndPage(oldPagePos, chunk, start, pageLength, type);
    }
}
