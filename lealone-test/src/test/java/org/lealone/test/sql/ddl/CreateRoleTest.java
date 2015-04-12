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
import org.lealone.test.sql.TestBase;

public class CreateRoleTest extends TestBase {
    @Test
    public void run() {
        executeUpdate("DROP TABLE IF EXISTS CreateRoleTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS CreateRoleTest (f1 int)");

        executeUpdate("CREATE ROLE IF NOT EXISTS myrole1");

        executeUpdate("CREATE ROLE IF NOT EXISTS myrole0");
        executeUpdate("GRANT INSERT ON CreateRoleTest TO myrole0");

        executeUpdate("GRANT SELECT,DELETE ON CreateRoleTest TO myrole1");
        executeUpdate("GRANT myrole1 TO myrole0");

        executeUpdate("CREATE USER IF NOT EXISTS sa1 PASSWORD 'abc' ADMIN");

        executeUpdate("GRANT myrole1 TO sa1");

        executeUpdate("DROP ROLE IF EXISTS myrole1");

        executeUpdate("CREATE ROLE IF NOT EXISTS myrole3");
        executeUpdate("DROP ROLE IF EXISTS myrole3");
        executeUpdate("CREATE ROLE IF NOT EXISTS myrole4");
        executeUpdate("DROP ROLE IF EXISTS myrole4");
    }
}
