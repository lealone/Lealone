/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import org.junit.Test;
import org.lealone.db.index.standard.ValueDataType;
import org.lealone.db.index.standard.VersionedValue;
import org.lealone.db.index.standard.VersionedValueType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueString;
import org.lealone.storage.CursorParameters;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.page.PageStorageMode;
import org.lealone.transaction.aote.TransactionalValue;
import org.lealone.transaction.aote.TransactionalValueType;

public class PageStorageModeTest extends AoseTestBase {

    private final int rowCount = 6000;
    private final int columnCount = 10;
    private final int pageSplitSize = 1024 * 1024;
    private final int cacheSize = 100; // 100M

    @Test
    public void run() {
        ValueDataType keyType = new ValueDataType(null, null, null);
        VersionedValueType vvType = new VersionedValueType(null, null, null, columnCount);
        TransactionalValueType tvType = new TransactionalValueType(vvType);

        testRowStorage(keyType, tvType);
        testColumnStorage(keyType, tvType);
    }

    private void testRowStorage(ValueDataType keyType, TransactionalValueType tvType) {
        testStorage(keyType, tvType, PageStorageMode.ROW_STORAGE, "testRowStorage");
    }

    private void testColumnStorage(ValueDataType keyType, TransactionalValueType tvType) {
        testStorage(keyType, tvType, PageStorageMode.COLUMN_STORAGE, "testColumnStorage");
    }

    private void putData(StorageMap<ValueLong, TransactionalValue> map) {
        if (!map.isEmpty())
            return;
        for (int row = 1; row <= rowCount; row++) {
            ValueLong key = ValueLong.get(row);
            Value[] columns = new Value[columnCount];
            for (int col = 0; col < columnCount; col++) {
                columns[col] = ValueString.get("value-row" + row + "-col" + (col + 1));
            }
            VersionedValue vv = new VersionedValue(row, columns);
            TransactionalValue tv = TransactionalValue.createCommitted(vv);
            map.put(key, tv);
        }
        map.save();
    }

    private void testStorage(ValueDataType keyType, TransactionalValueType tvType, PageStorageMode mode,
            String mapName) {
        AOStorage storage = AOStorageTest.openStorage(pageSplitSize, cacheSize);
        BTreeMap<ValueLong, TransactionalValue> map = storage.openBTreeMap(mapName, keyType, tvType,
                null);
        map.setPageStorageMode(mode);
        putData(map);

        ValueLong firstKey = map.firstKey();
        assertEquals(1, firstKey.getLong());

        int columnIndex = 2; // 索引要从0开始算

        ValueLong key = ValueLong.get(4000);
        TransactionalValue tv = map.get(key);
        VersionedValue vv = (VersionedValue) tv.getValue();
        Value columnValue = vv.columns[columnIndex];
        assertEquals("value-row4000-col3", columnValue.getString());

        key = ValueLong.get(2);
        tv = map.get(key, columnIndex);
        vv = (VersionedValue) tv.getValue();
        columnValue = vv.columns[columnIndex];
        assertEquals("value-row2-col3", columnValue.getString());

        key = ValueLong.get(2999);
        tv = map.get(key, columnIndex);
        vv = (VersionedValue) tv.getValue();
        columnValue = vv.columns[columnIndex];
        assertEquals("value-row2999-col3", columnValue.getString());

        int rows = 0;
        ValueLong from = ValueLong.get(2000);
        StorageMapCursor<ValueLong, TransactionalValue> cursor = map
                .cursor(CursorParameters.create(from, columnIndex));
        while (cursor.next()) {
            rows++;
        }
        assertEquals(rowCount - 2000 + 1, rows);
        map.close();
    }
}
