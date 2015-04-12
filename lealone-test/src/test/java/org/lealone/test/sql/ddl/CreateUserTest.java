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

import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;
import org.lealone.test.TestBase;

public class CreateUserTest extends TestBase {
    @Test
    public void run() {
        executeUpdate("DROP ROLE IF EXISTS sa1");
        // executeUpdate("CREATE ROLE IF NOT EXISTS sa1");

        executeUpdate("DROP USER IF EXISTS sa1 CASCADE");
        //executeUpdate("DROP USER IF EXISTS SA2 CASCADE");
        executeUpdate("DROP USER IF EXISTS SA3 CASCADE");

        executeUpdate("DROP SCHEMA IF EXISTS TEST_SCHEMA2");
        executeUpdate("DROP USER IF EXISTS SA222 CASCADE");

        executeUpdate("CREATE USER IF NOT EXISTS sa1 PASSWORD 'abc' ADMIN");
        //X不加也是可以的
        executeUpdate("CREATE USER IF NOT EXISTS SA2 SALT X'123456' HASH X'78' ADMIN"); // X'...'必须是偶数个
        executeUpdate("CREATE USER IF NOT EXISTS SA3 IDENTIFIED BY abc"); // 密码不加引号

        executeUpdate("CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA2 AUTHORIZATION SA2");

        executeUpdate("DROP USER IF EXISTS guest");
        executeUpdate("CREATE USER IF NOT EXISTS guest COMMENT 'create a guest user' PASSWORD 'abc'");

        executeUpdate("ALTER USER SA2 SET PASSWORD '123'");
        executeUpdate("ALTER USER SA2 SET SALT X'123456' HASH X'78'");

        executeUpdate("ALTER USER SA2 RENAME TO SA222");
        try {
            stmt.executeUpdate("ALTER USER SA222 ADMIN false");
            Assert.fail("not throw SQLException");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("cannot drop"));
        }
        //rightTest();
    }

    void rightTest() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS CreateUserTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS CreateUserTest (f1 int)");
        //executeUpdate("CREATE TABLE IF NOT EXISTS CreateUserTest (f1 IDENTITY)");
        //executeUpdate("CREATE TABLE IF NOT EXISTS CreateUserTest (f1 IDENTITY(1,10))");
        //executeUpdate("CREATE TABLE IF NOT EXISTS CreateUserTest (f1 SERIAL(1,10)))");

        //executeUpdate("CREATE TABLE IF NOT EXISTS CreateUserTest (f1 IDENTITY(1,10),PRIMARY KEY(f1))");
        //executeUpdate("CREATE TABLE IF NOT EXISTS CreateUserTest (f1 int,PRIMARY KEY(f1))");

        //executeUpdate("CREATE TABLE IF NOT EXISTS CreateUserTest (f1 int,CONSTRAINT IF NOT EXISTS my_constraint COMMENT IS 'haha' INDEX int)");

        //executeUpdate("CREATE TABLE IF NOT EXISTS CreateUserTest (f1 int,f2 int,CONSTRAINT IF NOT EXISTS my_constraint COMMENT IS 'haha' INDEX my_int(f1,f2))");
        //executeUpdate("CREATE TABLE IF NOT EXISTS TEST9.public.CreateUserTest (f1 int,f2 int,"
        //      + "CONSTRAINT IF NOT EXISTS my_constraint COMMENT IS 'haha' CHECK f1>0)");
        //executeUpdate("CREATE TABLE IF NOT EXISTS CreateUserTest (f1 int,f2 int,"
        //      + "CONSTRAINT IF NOT EXISTS my_constraint COMMENT IS 'haha' UNIQUE KEY INDEX my_constraint2(f1,f2) INDEX myi)");
        //executeUpdate("CREATE TABLE IF NOT EXISTS CreateUserTest (f1 int,f2 int,"
        //      + "CONSTRAINT IF NOT EXISTS my_constraint COMMENT IS 'haha' FOREIGN KEY(f1,f2))  INDEX my-i REFERENCES(f1)");

        executeUpdate("CREATE ROLE IF NOT EXISTS myrole1");
        executeUpdate("CREATE ROLE IF NOT EXISTS myrole2");
        executeUpdate("CREATE ROLE IF NOT EXISTS myrole3");

        //GRANT
        executeUpdate("GRANT SELECT,DELETE,INSERT ON CreateUserTest TO PUBLIC");
        executeUpdate("GRANT UPDATE ON CreateUserTest TO PUBLIC");
        executeUpdate("GRANT SELECT,DELETE,INSERT,UPDATE ON CreateUserTest TO SA2");
        executeUpdate("GRANT SELECT,DELETE,INSERT,UPDATE ON CreateUserTest TO myrole1");

        executeUpdate("GRANT myrole1 TO myrole2");
        //executeUpdate("GRANT myrole2 TO myrole2");
        executeUpdate("GRANT myrole2 TO myrole1");
        executeUpdate("GRANT myrole1 TO myrole3");
        executeUpdate("GRANT myrole3 TO PUBLIC");

        executeUpdate("GRANT myrole1 TO PUBLIC");
        executeUpdate("GRANT myrole1 TO SA3");
        executeUpdate("GRANT myrole1 TO myrole2");
        //executeUpdate("GRANT myrole2 TO myrole2");//cyclic role grants are not allowed

        //REVOKE
        executeUpdate("REVOKE SELECT,DELETE,INSERT,UPDATE ON CreateUserTest FROM PUBLIC");
        executeUpdate("REVOKE SELECT,DELETE,INSERT,UPDATE ON CreateUserTest FROM SA2");
        executeUpdate("REVOKE SELECT,DELETE,INSERT,UPDATE ON CreateUserTest FROM myrole1");

        executeUpdate("REVOKE myrole1 FROM PUBLIC");
        executeUpdate("REVOKE myrole1 FROM SA3");
        executeUpdate("REVOKE myrole1 FROM myrole2");
    }
}
