/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import org.junit.Test;
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
        // 弄个子目录，避免跟其他测试冲突，encryptionKey也会影响其他测试
        // 相同的storagePath会得到同样的Storage实例
        String storagePath = joinDirs("aose", "AOStorageTest");
        storage = openStorage(builder, storagePath);
    }

    private void testDrop() {
        BTreeMap<Integer, String> map = storage.openBTreeMap("AOStorageTest_testDrop");
        for (int i = 1; i <= 100; i++) {
            map.put(i, "value" + i);
        }
        storage.drop();
        assertEquals(0, storage.getMapNames().size());
        FilePath p = FilePath.get(storage.getStoragePath());
        assertTrue(!p.exists());
    }

    private void testBackupTo() {
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

    private void testOpenMap() {
        storage.openBTreeMap("AOStorageTest_map1");
        storage.openBTreeMap("AOStorageTest_map2", null, null, null);

        assertEquals(2, storage.getMapNames().size());
        assertTrue(storage.hasMap("AOStorageTest_map1"));
        assertTrue(storage.hasMap("AOStorageTest_map2"));

        storage.closeMap("AOStorageTest_map1");
        assertEquals(1, storage.getMapNames().size());
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
        return openStorage(builder, null);
    }

    public static AOStorage openStorage(AOStorageBuilder builder, String storagePath) {
        if (storagePath == null)
            storagePath = joinDirs("aose");
        builder.compressHigh();
        builder.storagePath(storagePath).minFillRate(30);
        AOStorage storage = builder.openStorage();
        return storage;
    }
}
