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
import com.lealone.storage.StorageMap;
import com.lealone.storage.StorageSetting;
import com.lealone.storage.aose.AOStorage;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.aose.btree.page.PageStorageMode;

public class CompactTest extends AoseTestBase {
    @Test
    public void run() {
        runRowStorageCompact();
        runColumnStorageCompact();
    }

    void runRowStorageCompact() {
        init();

        // map.clear();
        //
        // map.put(1, "v1");
        // map.put(50, "v50");
        // map.put(100, "v100");

        // map.save();

        putData(1, 200);
        putData(1, 50);
        putData(100, 150);

        new Thread(() -> {
            assertEquals(map.cursor(), 200);
        }).start();

        putData(50, 150);

        assertEquals(map.cursor(), 200);

        assertEquals(200, map.size());
    }

    private void putData(int rowStart, int rowEnd) {
        for (int i = rowStart; i <= rowEnd; i++)
            map.put(i, "value" + i);
        map.save();
        // map.printPage();
    }

    private void putData(StorageMap<Integer, Row> map, int columnCount, int rowStart, int rowEnd) {
        for (int row = rowStart; row <= rowEnd; row++) {
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

    void runColumnStorageCompact() {
        int columnCount = 10;
        int pageSize = 256 * 1024;
        int cacheSize = 100; // 100M

        AOStorage storage = openStorage(pageSize, cacheSize);
        RowType valueType = new RowType(null, columnCount);
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(StorageSetting.PAGE_STORAGE_MODE.name(), PageStorageMode.COLUMN_STORAGE.name());
        BTreeMap<Integer, Row> map = storage.openBTreeMap("ColumnStorageCompact", null, valueType,
                parameters);

        putData(map, columnCount, 1, 2000);
        putData(map, columnCount, 1, 500);
        putData(map, columnCount, 1000, 1500);
        putData(map, columnCount, 500, 1500);

        assertEquals(map.cursor(), 2000);

        assertEquals(2000, map.size());
    }
}
