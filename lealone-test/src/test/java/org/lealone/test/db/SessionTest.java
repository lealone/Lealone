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
import org.lealone.db.ServerSessionFactory;
import org.lealone.db.Session;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.test.UnitTestBase;

public class SessionTest extends UnitTestBase {

    @Test
    public void run() {
        setInMemory(true);
        setEmbedded(true);

        String url = getURL();
        Session session = ServerSessionFactory.getInstance().createSession(url);

        String sql = "CREATE TABLE IF NOT EXISTS SessionTest(f1 int, f2 int)";
        int fetchSize = 0;
        PreparedSQLStatement ps = session.prepareStatement(sql, fetchSize);
        p(ps.isQuery());
    }
}
