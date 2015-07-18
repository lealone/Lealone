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
package org.lealone.test.transaction;

import org.junit.Test;
import org.lealone.storage.StorageMap;
import org.lealone.test.UnitTestBase;
import org.lealone.test.storage.MemoryStorageEngine;
import org.lealone.transaction.MVCCTransaction;
import org.lealone.transaction.MVCCTransactionEngine;
import org.lealone.transaction.MVCCTransactionMap;
import org.lealone.type.ObjectDataType;

public class MVCCTransactionTest extends UnitTestBase {
    @Test
    public void run() {
        StorageMap.Builder mapBuilder = new MemoryStorageEngine.MemoryMapBuilder();
        String hostAndPort = getHost() + ":" + getPort();
        MVCCTransactionEngine e = new MVCCTransactionEngine(new ObjectDataType(), mapBuilder, hostAndPort, false);
        e.init(null);

        MVCCTransaction t = e.beginTransaction(false);
        MVCCTransactionMap<String, String> map = t.openMap("test");
        map.put("1", "a");
        map.put("2", "b");
        assertEquals("a", map.get("1"));
        assertEquals("b", map.get("2"));

        t.rollback();

        t = e.beginTransaction(false);

        assertNull(map.get("1"));
        assertNull(map.get("2"));
        map = map.getInstance(t, Long.MAX_VALUE);
        map.put("1", "a");
        map.put("2", "b");
        t.commit();

        assertEquals(2, map.sizeAsLong());
    }
}
