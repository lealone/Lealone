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
package org.lealone.test.engine;

import org.junit.Test;
import org.lealone.engine.ConnectionInfo;
import org.lealone.test.UnitTestBase;

public class ConnectionInfoTest extends UnitTestBase {
    @Test
    public void run() {
        try {
            setEmbedded(true);

            ConnectionInfo ci = new ConnectionInfo(getURL());

            assertTrue(ci.isEmbedded());
            assertTrue(ci.isPersistent());
            assertFalse(ci.isRemote());
            assertTrue(ci.getDatabaseName() != null && ci.getDatabaseName().endsWith(DB_NAME));
            assertNull(ci.getServer());

            setEmbedded(false);

            ci = new ConnectionInfo(getURL());
            assertFalse(ci.isEmbedded());
            assertFalse(ci.isPersistent()); //TCP类型的URL在Client端建立连接时无法确定是否是Persistent，所以为false
            assertTrue(ci.isRemote());
            assertEquals(DB_NAME, ci.getDatabaseName());
            assertEquals(getHostAndPort(), ci.getServer());

            try {
                new ConnectionInfo("invalid url");
                fail();
            } catch (Exception e) {
            }

            setMysqlUrlStyle(true);
            try {
                new ConnectionInfo(getURL() + ";a=b"); //MySQL风格的URL中不能出现';'
                fail();
            } catch (Exception e) {
            }

            setMysqlUrlStyle(false);
            try {
                new ConnectionInfo(getURL() + "&a=b"); //默认风格的URL中不能出现'&'
                fail();
            } catch (Exception e) {
            }

            setEmbedded(true);
            try {
                new ConnectionInfo(getURL(), "mydb"); //传递到Server端构建ConnectionInfo时URL不会是嵌入式的
                fail();
            } catch (Exception e) {
            }
        } finally {
            reset();
        }
    }
}
