/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.AOStorageBuilder;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.test.TestBase;

public abstract class AoseTestBase extends TestBase implements TestBase.EmbeddedTest {

    protected AOStorage storage;
    protected BTreeMap<Integer, String> map;
    protected int pageSize = 1 * 1024;

    protected void init() {
        init(false);
    }

    protected void init(boolean clearMap) {
        init(getClass().getSimpleName(), clearMap);
    }

    protected void init(String mapName) {
        init(mapName, false);
    }

    protected void init(String mapName, boolean clearMap) {
        storage = openStorage(pageSize);
        map = storage.openBTreeMap(mapName);
        if (clearMap)
            map.clear();
    }

    protected void openMap() {
        if (map == null || map.isClosed()) {
            map = storage.openBTreeMap(getClass().getSimpleName());
        }
    }

    protected void assertEquals(StorageMapCursor<?, ?> cursor, int expectedSsize) {
        AtomicInteger count = new AtomicInteger();
        cursor.forEachRemaining(e -> {
            count.incrementAndGet();
        });
        assertEquals(expectedSsize, count.get());
    }

    public static AOStorage openStorage() {
        AOStorageBuilder builder = new AOStorageBuilder();
        return openStorage(builder);
    }

    public static AOStorage openStorage(int pageSize) {
        AOStorageBuilder builder = new AOStorageBuilder();
        builder.pageSize(pageSize);
        return openStorage(builder);
    }

    public static AOStorage openStorage(int pageSize, int cacheSize) {
        AOStorageBuilder builder = new AOStorageBuilder();
        builder.pageSize(pageSize);
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
