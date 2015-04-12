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
package org.lealone.test.sql.ddl;

import org.junit.Test;
import org.lealone.test.TestBase;

public class SetCommentTest extends TestBase {
    @Test
    public void run() {
        executeUpdate("DROP TABLE IF EXISTS SetCommentTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS SetCommentTest (f1 int)");

        executeUpdate("DROP ROLE IF EXISTS myrole");
        executeUpdate("CREATE ROLE IF NOT EXISTS myrole");

        executeUpdate("COMMENT ON COLUMN " + db + ".public.SetCommentTest.f1 IS 'column comment'");
        executeUpdate("COMMENT ON TABLE public.SetCommentTest IS 'table comment'");

        executeUpdate("COMMENT ON ROLE myrole IS 'role comment'");
    }
}
