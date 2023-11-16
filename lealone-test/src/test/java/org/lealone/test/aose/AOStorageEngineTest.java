/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import org.junit.Test;
import org.lealone.db.PluginManager;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageBuilder;
import org.lealone.storage.StorageEngine;
import org.lealone.storage.aose.AOStorageBuilder;
import org.lealone.storage.aose.AOStorageEngine;
import org.lealone.storage.aose.lob.LobStreamStorage;
import org.lealone.storage.lob.LobStorage;

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
