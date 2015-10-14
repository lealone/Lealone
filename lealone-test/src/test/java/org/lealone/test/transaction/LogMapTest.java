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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.lealone.test.UnitTestBase;
import org.lealone.transaction.log.LogMap;
import org.lealone.transaction.log.LogStorage;

public class LogMapTest extends UnitTestBase {
    @Test
    public void run() {
        Map<String, String> config = new HashMap<>();
        config.put("base_dir", joinDirs("transaction-test"));
        config.put("transaction_log_dir", "tlog");
        config.put("log_sync_type", "none");
        config.put("log_chunk_size", "128");

        LogStorage ls = new LogStorage(config);
        LogMap<Integer, Integer> map = ls.openLogMap("test", null, null);// 自动侦测key/value的类型

        for (int i = 10; i < 100; i++) {
            if (i > 10 && i % 10 == 0)
                map.save();
            map.put(i, i * 10);
        }

        assertEquals(10 * 10, map.get(10).intValue());
        assertEquals(50 * 10, map.get(50).intValue());
        assertEquals(99 * 10, map.get(99).intValue());

        assertNull(map.get(100));

        map.remove();
        ls.close();
    }
}
