/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.aose.btree.chunk.Chunk;
import com.lealone.storage.type.StorageDataType;

public abstract class ColumnStorageLeafPage extends LeafPage {

    protected PageReference[] columnPages;
    protected boolean isAllColumnPagesRead;

    public ColumnStorageLeafPage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public Object getValue(int index) {
        return getValue(index, true);
    }

    @Override
    public Object getValue(int index, int[] columnIndexes) {
        if (columnPages != null && columnIndexes != null && !isAllColumnPagesRead) {
            for (int columnIndex : columnIndexes) {
                readColumnPage(columnIndex);
            }
        }
        return getValues()[index];
    }

    @Override
    public Object getValue(int index, boolean allColumns) {
        if (columnPages != null && allColumns && !isAllColumnPagesRead) {
            readAllColumnPages();
        }
        return getValues()[index];
    }

    private void readAllColumnPages() {
        for (int columnIndex = 0, len = columnPages.length; columnIndex < len; columnIndex++) {
            readColumnPage(columnIndex);
        }
        isAllColumnPagesRead = true;
    }

    private void readColumnPage(int columnIndex) {
        PageReference ref = columnPages[columnIndex];
        ColumnPage page = (ColumnPage) ref.getOrReadPage();
        if (page.getMemory() <= 0)
            page.readColumn(getValues(), columnIndex);
    }

    protected void markAllColumnPagesDirty() {
        if (columnPages != null) {
            if (!isAllColumnPagesRead) {
                readAllColumnPages();
            }
            for (PageReference ref : columnPages) {
                if (ref != null && ref.getPage() != null) {
                    ref.markDirtyPage();
                }
            }
            columnPages = null;
        }
    }

    @Override
    public Object setValue(int index, Object value) {
        if (columnPages != null)
            markAllColumnPagesDirty();
        return super.setValue(index, value);
    }

    protected abstract void readValues(ByteBuffer buff, int keyLength, int columnCount);

    @Override
    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);
        buff.get(); // mode
        readCheckValue(buff, chunkId, offset, pageLength);

        int keyLength = DataUtils.readVarInt(buff);
        int columnCount = DataUtils.readVarInt(buff);
        columnPages = new PageReference[columnCount];
        keys = new Object[keyLength];
        int type = buff.get();
        for (int i = 0; i < columnCount; i++) {
            long pos = buff.getLong();
            columnPages[i] = new PageReference(map.getBTreeStorage(), pos);
        }
        buff = expandPage(buff, type, start, pageLength);

        map.getKeyType().read(buff, keys, keyLength);
        readValues(buff, keyLength, columnCount);
        buff.getInt(); // replicationHostIds
        recalculateMemory();
        // 延迟加载列
    }

    @Override
    public long write(PageInfo pInfoOld, Chunk chunk, DataBuffer buff, AtomicBoolean isLocked) {
        beforeWrite(pInfoOld);
        int start = buff.position();
        int keyLength = keys.length;
        int type = PageUtils.PAGE_TYPE_LEAF;
        buff.putInt(0); // 回填pageLength
        buff.put((byte) PageStorageMode.COLUMN_STORAGE.ordinal());
        StorageDataType valueType = map.getValueType();
        int columnCount = valueType.getColumnCount();
        int checkPos = buff.position();
        buff.putShort((short) 0).putVarInt(keyLength).putVarInt(columnCount);
        int typePos = buff.position();
        buff.put((byte) type);
        int columnPageStartPos = buff.position();
        for (int i = 0; i < columnCount; i++) {
            buff.putLong(0);
        }
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, keyLength);
        Object[] values = getValues();
        boolean isLockable = valueType.isLockable();
        boolean isLockedPage = false;
        for (int row = 0; row < keyLength; row++) {
            valueType.writeMeta(buff, values[row]);
            if (isLockable && !isLockedPage)
                isLockedPage = isLocked(values[row]);
        }
        if (isLockedPage)
            isLocked.set(true);
        buff.putInt(0); // replicationHostIds
        compressPage(buff, compressStart, type, typePos);

        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);

        writeCheckValue(buff, chunk, start, pageLength, checkPos);

        long[] posArray = new long[columnCount];
        for (int col = 0; col < columnCount; col++) {
            ColumnPage page = new ColumnPage(map);
            page.setRef(new PageReference(map.getBTreeStorage(), 0));
            posArray[col] = page.write(chunk, buff, values, col);
        }
        int oldPos = buff.position();
        buff.position(columnPageStartPos);
        for (int i = 0; i < columnCount; i++) {
            buff.putLong(posArray[i]);
        }
        buff.position(oldPos);

        return updateChunkAndPage(pInfoOld, chunk, start, pageLength, type, isLockedPage, true);
    }
}
