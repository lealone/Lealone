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
import com.lealone.storage.aose.btree.BTreeStorage;
import com.lealone.storage.aose.btree.chunk.Chunk;
import com.lealone.storage.type.StorageDataType;

public abstract class ColumnStorageLeafPage extends LeafPage {

    private PageReference[] columnPages;
    private boolean isAllColumnPagesRead;
    private int formatVersionForRead;

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
            if (columnIndexes.length >= columnPages.length) {
                boolean allRead = true;
                for (PageReference ref : columnPages) {
                    if (ref.getPage() == null) {
                        allRead = false;
                        break;
                    }
                }
                isAllColumnPagesRead = allRead;
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
        if (columnIndex < 0) // _rowid_ 直接忽略
            return;
        else if (columnIndex >= columnPages.length) // 新增加的列
            return;
        PageReference ref = columnPages[columnIndex];
        ColumnPage page = (ColumnPage) ref.getOrReadPage();
        if (page.getMemory() <= 0)
            page.readColumn(getValues(), columnIndex, formatVersionForRead);
    }

    void markAllColumnPagesDirty() {
        if (columnPages != null) {
            if (!isAllColumnPagesRead) {
                readAllColumnPages();
            }
            for (PageReference ref : columnPages) {
                if (ref.getPos() > 0) {
                    ref.markDirtyPage();
                }
            }
            columnPages = null;
        }
    }

    protected abstract void readValues(ByteBuffer buff, int keyLength, int columnCount,
            int formatVersion);

    @Override
    public int read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength) {
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

        Chunk chunk = map.getBTreeStorage().getChunkManager().getChunk(chunkId);
        map.getKeyType().read(buff, keys, keyLength, chunk.formatVersion);
        readValues(buff, keyLength, columnCount, chunk.formatVersion);
        buff.getInt(); // replicationHostIds
        int metaVersion = 0;
        if (chunk.isNewFormatVersion())
            metaVersion = DataUtils.readVarInt(buff); // metaVersion
        recalculateMemory();
        formatVersionForRead = chunk.formatVersion;
        // 延迟加载列
        return metaVersion;
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
        map.getKeyType().write(buff, keys, keyLength, chunk.formatVersion);
        Object[] values = getValues();
        boolean isLockable = valueType.isLockable();
        boolean isLockedPage = false;
        for (int row = 0; row < keyLength; row++) {
            valueType.writeMeta(buff, values[row], chunk.formatVersion);
            if (isLockable && !isLockedPage)
                isLockedPage = isLocked(values[row]);
        }
        if (isLockedPage)
            isLocked.set(true);
        buff.putInt(0); // replicationHostIds
        if (chunk.isNewFormatVersion())
            buff.putVarInt(pInfoOld.metaVersion);
        compressPage(buff, compressStart, type, typePos);

        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);

        writeCheckValue(buff, chunk, start, pageLength, checkPos);

        columnPages = new PageReference[columnCount];
        isAllColumnPagesRead = true;
        long[] posArray = new long[columnCount];
        for (int col = 0; col < columnCount; col++) {
            ColumnPage page = new ColumnPage(map);
            columnPages[col] = new PageReference(map.getBTreeStorage(), page);
            page.setRef(columnPages[col]);
            posArray[col] = page.write(chunk, buff, values, col);
        }
        writeColumnPagePositions(buff, columnPageStartPos, columnCount, posArray);

        return updateChunkAndPage(pInfoOld, chunk, start, pageLength, type, true, isLockedPage);
    }

    private static void writeColumnPagePositions(DataBuffer buff, int columnPageStartPos,
            int columnCount, long[] posArray) {
        int oldPos = buff.position();
        buff.position(columnPageStartPos);
        for (int i = 0; i < columnCount; i++) {
            buff.putLong(posArray[i]);
        }
        buff.position(oldPos);
    }

    // 重写所有的ColumnStorageLeafPage，不但要修改CheckValue，还需要修改它所包含的所有ColumnPage的pos
    public static long rewrite(BTreeStorage bs, Chunk chunk, DataBuffer buff, ByteBuffer pageBuff,
            int pageLength) {
        pageBuff.position(7); // 跳过前7个字节
        DataUtils.readVarInt(pageBuff); // keyLength
        int columnCount = DataUtils.readVarInt(pageBuff);
        pageBuff.position(pageBuff.position() + 1); // 跳过type

        int columnPageStartPos = buff.position() + pageBuff.position();

        // 读取所有ColumnPage的pos
        long[] posArray = new long[columnCount];
        for (int i = 0; i < columnCount; i++) {
            posArray[i] = pageBuff.getLong();
        }

        pageBuff.position(0);
        long pos = LeafPage.rewrite(chunk, buff, pageBuff, pageLength, 5, PageUtils.PAGE_TYPE_LEAF);

        // 修改它所包含的所有ColumnPage的pos
        for (int i = 0; i < columnCount; i++) {
            long posNew = ColumnPage.rewrite(bs, chunk, buff, posArray[i]);
            posArray[i] = posNew;
        }
        writeColumnPagePositions(buff, columnPageStartPos, columnCount, posArray);
        return pos;
    }
}
