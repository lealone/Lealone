/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.aose;

import java.util.HashMap;

import org.junit.Test;

import com.lealone.db.row.Row;
import com.lealone.db.row.RowType;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueString;
import com.lealone.storage.CursorParameters;
import com.lealone.storage.StorageMap;
import com.lealone.storage.StorageMapCursor;
import com.lealone.storage.StorageSetting;
import com.lealone.storage.aose.AOStorage;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.aose.btree.page.PageStorageMode;

public class PageStorageModeTest extends AoseTestBase {

    private final int rowCount = 6000;
    private final int columnCount = 10;
    private final int pageSize = 1024 * 1024;
    private final int cacheSize = 100; // 100M

    @Test
    public void run() {
        testStorage(PageStorageMode.ROW_STORAGE, "testRowStorage");
        testStorage(PageStorageMode.COLUMN_STORAGE, "testColumnStorage");
    }

    private void putData(StorageMap<Integer, Row> map) {
        if (!map.isEmpty())
            return;
        for (int row = 1; row <= rowCount; row++) {
            int key = row;
            Value[] columns = new Value[columnCount];
            for (int col = 0; col < columnCount; col++) {
                columns[col] = ValueString.get("value-row" + row + "-col" + (col + 1));
            }
            Row r = new Row(columns);
            map.put(key, r);
        }
        map.save();
    }

    private void testStorage(PageStorageMode mode, String mapName) {
        AOStorage storage = openStorage(pageSize, cacheSize);
        RowType valueType = new RowType(null, columnCount);
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(StorageSetting.PAGE_STORAGE_MODE.name(), mode.name());
        BTreeMap<Integer, Row> map = storage.openBTreeMap(mapName, null, valueType, parameters);
        putData(map);

        int firstKey = map.firstKey();
        assertEquals(1, firstKey);

        int columnIndex = 2; // 索引是从0开始算

        int key = 4000;
        Row r = map.get(key);
        Value columnValue = r.getColumns()[columnIndex];
        assertEquals("value-row4000-col3", columnValue.getString());

        key = 2;
        r = map.get(key, columnIndex);
        columnValue = r.getColumns()[columnIndex];
        assertEquals("value-row2-col3", columnValue.getString());

        key = 2999;
        r = map.get(key, columnIndex);
        columnValue = r.getColumns()[columnIndex];
        assertEquals("value-row2999-col3", columnValue.getString());

        int rows = 0;
        Integer from = 2000;
        StorageMapCursor<Integer, Row> cursor = map.cursor(CursorParameters.create(from, columnIndex));
        while (cursor.next()) {
            rows++;
        }
        assertEquals(rowCount - 2000 + 1, rows);
        map.close();
    }
}
