/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.plugins;

import org.junit.Test;
import org.lealone.db.PluginManager;
import org.lealone.storage.StorageEngine;
import org.lealone.storage.aose.AOStorageEngine;
import org.lealone.test.TestBase;

public class PluginManagerTest extends TestBase {
    @Test
    public void run() {
        StorageEngine se = PluginManager.getPlugin(StorageEngine.class, AOStorageEngine.NAME);
        StorageEngine old = se;
        assertTrue(se instanceof AOStorageEngine);

        // 默认是用StorageEngine.class为key，所以用AOStorageEngine.class时找不到
        se = PluginManager.getPlugin(AOStorageEngine.class, AOStorageEngine.NAME);
        assertNull(se);

        PluginManager.deregister(StorageEngine.class, old);
        se = PluginManager.getPlugin(StorageEngine.class, AOStorageEngine.NAME);
        assertNull(se);

        se = new AOStorageEngine();
        PluginManager.register(se, "myaose");
        se = PluginManager.getPlugin(AOStorageEngine.class, "myaose");
        assertNotNull(se);

        // 重新注册回去，避免集成测试时影响其他测试用例
        PluginManager.register(StorageEngine.class, old);
        se = PluginManager.getPlugin(StorageEngine.class, AOStorageEngine.NAME);
        assertNotNull(se);
    }
}
