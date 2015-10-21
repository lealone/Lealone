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
package org.lealone.test.db;

import org.junit.Test;
import org.lealone.db.Setting;

public class SettingTest extends DbObjectTestBase {
    @Test
    public void run() {
        String name = "CACHE_SIZE";
        String sql = "SET " + name + " 1024";
        executeUpdate(sql);
        assertEquals(1024, db.getCacheSize());

        Setting s = db.findSetting(name);
        assertNotNull(s);
        assertTrue(s.getId() > 0);
        assertEquals(1024, s.getIntValue());

        db.removeDatabaseObject(session, s);
        s = db.findSetting(name);
        assertNull(s);
        session.commit(true);
    }
}
