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

import org.junit.Test;
import org.lealone.storage.LobStorage;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageBuilder;
import org.lealone.storage.StorageEngine;
import org.lealone.storage.StorageEngineManager;
import org.lealone.storage.aose.AOStorageBuilder;
import org.lealone.storage.aose.AOStorageEngine;
import org.lealone.storage.aose.lob.LobStreamStorage;
import org.lealone.test.TestBase;

public class AOStorageEngineTest extends TestBase {

    @Test
    public void run() {
        StorageEngine se = StorageEngineManager.getStorageEngine(AOStorageEngine.NAME);
        assertTrue(se instanceof AOStorageEngine);
        assertEquals(AOStorageEngine.NAME, se.getName());
        StorageBuilder builder = se.getStorageBuilder();
        assertTrue(builder instanceof AOStorageBuilder);

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
