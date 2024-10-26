/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.aose;

import org.junit.Test;

import com.lealone.db.plugin.PluginManager;
import com.lealone.storage.Storage;
import com.lealone.storage.StorageBuilder;
import com.lealone.storage.StorageEngine;
import com.lealone.storage.aose.AOStorageBuilder;
import com.lealone.storage.aose.AOStorageEngine;
import com.lealone.storage.aose.lob.LobStreamStorage;
import com.lealone.storage.lob.LobStorage;

public class AOStorageEngineTest extends AoseTestBase {
    @Test
    public void run() {
        StorageEngine se = PluginManager.getPlugin(StorageEngine.class, AOStorageEngine.NAME);
        assertTrue(se instanceof AOStorageEngine);
        assertEquals(AOStorageEngine.NAME, se.getName());
        StorageBuilder builder = se.getStorageBuilder();
        assertTrue(builder instanceof AOStorageBuilder);

        se = new AOStorageEngine();
        builder = se.getStorageBuilder();
        try {
            builder.openStorage(); // 还没有设置storagePath
            fail();
        } catch (Exception e) {
        }

        String storagePath = joinDirs("aose", "AOStorageEngineTest");
        builder.storagePath(storagePath);
        Storage storage = builder.openStorage();
        assertEquals(storagePath, storage.getStoragePath());

        LobStorage lobStorage = se.getLobStorage(null, storage);
        assertTrue(lobStorage instanceof LobStreamStorage);
    }
}
