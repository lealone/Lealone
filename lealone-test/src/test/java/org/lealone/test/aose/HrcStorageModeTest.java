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
package org.lealone.test.aose;

import org.lealone.db.index.standard.ValueDataType;
import org.lealone.db.index.standard.VersionedValue;
import org.lealone.db.index.standard.VersionedValueType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueString;
import org.lealone.storage.IterationParameters;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.PageStorageMode;
import org.lealone.test.TestBase;
import org.lealone.transaction.aote.TransactionalValue;
import org.lealone.transaction.aote.TransactionalValueType;

//把CACHE_SIZE加大后，RowStorage的方式有更多内存就不会重复从硬盘读取page，此时就跟ColumnStorage的性能差不多
public class HrcStorageModeTest extends TestBase {

    public static void main(String[] args) throws Exception {
        new HrcStorageModeTest().run();
    }

    int rowCount = 6000;
    int columnCount = 10;
    int pageSplitSize = 1024 * 1024;
    int cacheSize = 100 * 1024 * 1024; // 100M

    public void run() {
        ValueDataType keyType = new ValueDataType(null, null, null);
        ValueDataType valueType = new ValueDataType(null, null, null);
        VersionedValueType vvType = new VersionedValueType(valueType, columnCount);
        TransactionalValueType tvType = new TransactionalValueType(vvType);

        for (int i = 0; i < 10; i++) {
            System.out.println();
            System.out.println("------------------loop " + (i + 1) + " start---------------------");
            testRowStorage(keyType, tvType);

            System.out.println();
            testColumnStorage(keyType, tvType);
            System.out.println("------------------loop " + (i + 1) + " end---------------------");
        }
    }

    void putData(StorageMap<ValueLong, TransactionalValue> map) {
        if (map.isEmpty()) {
            // Random random = new Random();
            for (int row = 1; row <= rowCount; row++) {
                ValueLong key = ValueLong.get(row);
                Value[] columns = new Value[columnCount];
                for (int col = 0; col < columnCount; col++) {
                    // columns[col] = ValueString.get("a string");
                    // int randomIndex = random.nextInt(columnCount);
                    // columns[col] = ValueString.get("value-" + randomIndex);
                    // if (col % 2 == 0) {
                    // columns[col] = ValueString.get("a string");
                    // } else {
                    columns[col] = ValueString.get("value-row" + row + "-col" + (col + 1));
                    // }
                }
                // System.out.println(Arrays.asList(columns));
                VersionedValue vv = new VersionedValue(row, ValueArray.get(columns));
                TransactionalValue tv = TransactionalValue.createCommitted(vv);
                map.put(key, tv);
            }
            map.save();
            // map.remove();
        }
    }

    void testRowStorage(ValueDataType keyType, TransactionalValueType tvType) {
        long t0 = System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        AOStorage storage = AOStorageTest.openStorage(pageSplitSize, cacheSize);
        BTreeMap<ValueLong, TransactionalValue> map = storage.openBTreeMap("testRowStorage", keyType, tvType, null);
        map.setPageStorageMode(PageStorageMode.ROW_STORAGE);
        putData(map);
        long t2 = System.currentTimeMillis();
        System.out.println("RowStorage openBTreeMap time: " + (t2 - t1) + " ms");

        System.out.println("firstKey: " + map.firstKey());
        ValueLong key = ValueLong.get(2);
        int columnIndex = 2; // 索引要从0开始算
        TransactionalValue tv = map.get(key);
        VersionedValue vv = (VersionedValue) tv.getValue();
        System.out.println(vv.value.getList()[columnIndex]);
        t1 = System.currentTimeMillis();
        key = ValueLong.get(2999);
        tv = map.get(key);
        vv = (VersionedValue) tv.getValue();
        t2 = System.currentTimeMillis();
        System.out.println("RowStorage get time: " + (t2 - t1) + " ms");
        System.out.println(vv.value.getList()[columnIndex]);

        int rows = 0;
        ValueLong from = ValueLong.get(2000);
        t1 = System.currentTimeMillis();
        StorageMapCursor<ValueLong, TransactionalValue> cursor = map.cursor(from);
        while (cursor.hasNext()) {
            cursor.next();
            rows++;
        }
        t2 = System.currentTimeMillis();
        System.out.println("RowStorage cursor time: " + (t2 - t1) + " ms" + ", rows: " + rows);

        System.out.println("RowStorage total time: " + (t2 - t0) + " ms");
        // map.close(); //关闭之后就把缓存也关了
    }

    void testColumnStorage(ValueDataType keyType, TransactionalValueType tvType) {
        long t0 = System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        AOStorage storage = AOStorageTest.openStorage(pageSplitSize);
        BTreeMap<ValueLong, TransactionalValue> map = storage.openBTreeMap("testColumnStorage", keyType, tvType, null);
        map.setPageStorageMode(PageStorageMode.COLUMN_STORAGE);
        putData(map);
        long t2 = System.currentTimeMillis();
        System.out.println("ColumnStorage openBTreeMap time: " + (t2 - t1) + " ms");
        System.out.println("firstKey: " + map.firstKey());

        // t1 = System.currentTimeMillis();
        // map.get(ValueLong.get(4000));
        // t2 = System.currentTimeMillis();
        // System.out.println("hrcse get all columns time: " + (t2 - t1) + " ms");

        ValueLong key = ValueLong.get(2);
        int columnIndex = 2; // 索引要从0开始算
        TransactionalValue tv = map.get(key, columnIndex);
        VersionedValue vv = (VersionedValue) tv.getValue();
        System.out.println(vv.value.getList()[columnIndex]);
        t1 = System.currentTimeMillis();
        key = ValueLong.get(2999);
        tv = map.get(key, columnIndex);
        vv = (VersionedValue) tv.getValue();
        t2 = System.currentTimeMillis();
        System.out.println("ColumnStorage get time: " + (t2 - t1) + " ms");
        System.out.println(vv.value.getList()[columnIndex]);

        // key = ValueLong.get(2000);
        // tv = map.get(key, columnIndex);
        // vv = (VersionedValue) tv.value;
        // System.out.println(vv.value.getList()[columnIndex]);

        int rows = 0;
        ValueLong from = ValueLong.get(2000);
        t1 = System.currentTimeMillis();
        StorageMapCursor<ValueLong, TransactionalValue> cursor = map
                .cursor(IterationParameters.create(from, columnIndex));
        while (cursor.hasNext()) {
            cursor.next();
            rows++;
        }
        t2 = System.currentTimeMillis();
        System.out.println("ColumnStorage cursor time: " + (t2 - t1) + " ms" + ", rows: " + rows);

        System.out.println("ColumnStorage total time: " + (t2 - t0) + " ms");
        // map.close();
    }
}
