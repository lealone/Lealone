/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.lealone.test.aose;

import org.junit.Test;
import org.lealone.db.value.ValueString;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.AOStorageBuilder;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.fs.FilePath;
import org.lealone.test.TestBase;

public class AOStorageTest extends TestBase {

    private AOStorage storage;

    @Test
    public void run() {
        init();
        try {
            testOpenMap();
            testBackupTo();
            testDrop();
        } finally {
            storage.close();
        }
    }

    private void init() {
        AOStorageBuilder builder = new AOStorageBuilder();
        builder.pageSplitSize(1024);
        builder.encryptionKey("mykey".toCharArray());
        // builder.inMemory();
        storage = openStorage(builder);
    }

    void testDrop() {
        BTreeMap<Integer, String> map = storage.openBTreeMap("AOStorageTest_testDrop");
        for (int i = 1; i <= 100; i++) {
            map.put(i, "value" + i);
        }
        storage.drop();
        assertEquals(0, storage.getMapNames().size());
        FilePath p = FilePath.get(storage.getStoragePath());
        assertTrue(!p.exists());
    }

    void testBackupTo() {
        BTreeMap<Integer, String> map = storage.openBTreeMap("AOStorageTest_testBackupTo");
        for (int i = 1; i <= 100; i++) {
            map.put(i, "value" + i);
        }
        String fileName = joinDirs("testBackup", "backup1.zip");
        storage.backupTo(fileName);
        FilePath p = FilePath.get(fileName);
        assertTrue(p.exists());
        assertTrue(!p.isDirectory());
        assertTrue(p.size() > 0);
    }

    void testOpenMap() {
        storage.openBTreeMap("AOStorageTest_map1");
        storage.openRTreeMap("AOStorageTest_map2", ValueString.type, 3);
        storage.openBTreeMap("AOStorageTest_map3", null, null, null);
        storage.openAOMap("AOStorageTest_map4", null, null, null);

        assertEquals(4, storage.getMapNames().size());
        assertTrue(storage.hasMap("AOStorageTest_map1"));
        assertTrue(storage.hasMap("AOStorageTest_map2"));
        assertTrue(storage.hasMap("AOStorageTest_map3"));
        assertTrue(storage.hasMap("AOStorageTest_map4"));

        storage.closeMap("AOStorageTest_map1");
        assertEquals(3, storage.getMapNames().size());
        assertFalse(storage.hasMap("AOStorageTest_map1"));
        assertTrue(storage.nextTemporaryMapName().length() > 0);

        try {
            storage.openMap("xxx", "Unknow map type", null, null, null);
            fail();
        } catch (Exception e) {
        }
    }

    public static AOStorage openStorage() {
        AOStorageBuilder builder = new AOStorageBuilder();
        return openStorage(builder);
    }

    public static AOStorage openStorage(int pageSplitSize) {
        AOStorageBuilder builder = new AOStorageBuilder();
        builder.pageSplitSize(pageSplitSize);
        return openStorage(builder);
    }

    public static AOStorage openStorage(int pageSplitSize, int cacheSize) {
        AOStorageBuilder builder = new AOStorageBuilder();
        builder.pageSplitSize(pageSplitSize);
        builder.cacheSize(cacheSize);
        return openStorage(builder);
    }

    public static AOStorage openStorage(AOStorageBuilder builder) {
        String storagePath = joinDirs("aose");
        builder.compressHigh();
        builder.storagePath(storagePath).reuseSpace().minFillRate(30);
        return builder.openStorage();
    }
}
