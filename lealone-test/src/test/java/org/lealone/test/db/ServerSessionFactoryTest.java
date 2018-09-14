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
import org.lealone.common.exceptions.DbException;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.ServerSessionFactory;
import org.lealone.db.api.ErrorCode;
import org.lealone.test.UnitTestBase;

public class ServerSessionFactoryTest extends UnitTestBase {
    @Test
    public void run() {
        setInMemory(true);
        // setEmbedded(true);

        ConnectionInfo ci;
        try {
            // 只有嵌入式模式时才允许访问lealone数据库
            ci = new ConnectionInfo(getURL(LealoneDatabase.NAME));
            ServerSessionFactory.getInstance().createSession(ci);
            fail();
        } catch (DbException e) {
            assertEquals(ErrorCode.DATABASE_NOT_FOUND_1, e.getErrorCode());
        }
    }
}
