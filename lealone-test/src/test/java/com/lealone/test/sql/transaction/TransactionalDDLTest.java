/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.transaction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import com.lealone.common.security.SHA256;
import com.lealone.common.util.MathUtils;
import com.lealone.common.util.StringUtils;
import com.lealone.db.ConnectionSetting;
import com.lealone.db.Constants;
import com.lealone.test.TestBase.MainTest;
import com.lealone.test.sql.SqlTestBase;

public class TransactionalDDLTest extends SqlTestBase implements MainTest {
    public static void main(String[] args) throws Exception {
        new TransactionalDDLTest().runTest();
    }

    // @Test
    @Override
    public void test() throws Exception {
        conn.setAutoCommit(false);
        runDLL();
        // runDLLs();
        // conn.commit();
        conn.rollback();
        conn.close();
    }

    public void runDLLs() throws Exception {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            stmt.executeUpdate("CREATE USER IF NOT EXISTS sa" + i + " PASSWORD 'abc' ADMIN");
            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS table" + i + " (f1 int NOT NULL, f2 int, f3 varchar)");

            // for (int j = 0; j < 200; j++) {
            // executeUpdate("INSERT INTO table" + i + "(f1, f2, f3) VALUES(1, 1, '1')");
            // }
        }
        System.out.println("total time: " + (System.currentTimeMillis() - t1) + " ms");
    }

    public void runDLL() throws Exception {
        stmt.executeUpdate(
                "CREATE DATABASE IF NOT EXISTS db2 PARAMETERS(OPTIMIZE_DISTINCT=true, PERSISTENT=true)");

        stmt.executeUpdate("ALTER DATABASE db2 PARAMETERS(OPTIMIZE_DISTINCT=false)");

        // stmt.executeUpdate("CREATE DOMAIN IF NOT EXISTS EMAIL AS VARCHAR(255) CHECK (POSITION('@', VALUE) > 1)");
        // stmt.executeUpdate("CREATE FORCE AGGREGATE IF NOT EXISTS MEDIAN FOR \"" + MedianString.class.getName() +
        // "\"");
        //
        // stmt.executeUpdate("ALTER USER SA RENAME TO SA2");
        // stmt.executeUpdate("CREATE USER IF NOT EXISTS sa2 PASSWORD 'abc' ADMIN");
        // stmt.executeUpdate("DROP USER IF EXISTS sa2");
    }

    public void runDLL0() throws Exception {
        stmt.executeUpdate("DROP SCHEMA IF EXISTS TEST_SCHEMA2");

        stmt.executeUpdate("DROP ROLE IF EXISTS sa1");
        // stmt.executeUpdate("CREATE ROLE IF NOT EXISTS sa1");

        stmt.executeUpdate("DROP USER IF EXISTS SA222 CASCADE");
        stmt.executeUpdate("DROP USER IF EXISTS sa1 CASCADE");
        // stmt.executeUpdate("DROP USER IF EXISTS SA2 CASCADE");
        stmt.executeUpdate("DROP USER IF EXISTS SA3 CASCADE");

        stmt.executeUpdate("CREATE USER IF NOT EXISTS sa1 PASSWORD 'abc' ADMIN");
        // X不加也是可以的
        stmt.executeUpdate("CREATE USER IF NOT EXISTS SA2 SALT X'123456' HASH X'78' ADMIN"); // X'...'必须是偶数个
        stmt.executeUpdate("CREATE USER IF NOT EXISTS SA3 IDENTIFIED BY abc"); // 密码不加引号

        stmt.executeUpdate("CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA2 AUTHORIZATION SA2");

        stmt.executeUpdate("DROP USER IF EXISTS guest");
        stmt.executeUpdate(
                "CREATE USER IF NOT EXISTS guest COMMENT 'create a guest user' PASSWORD 'abc'");

        stmt.executeUpdate("ALTER USER SA2 SET PASSWORD '123'");
        stmt.executeUpdate("ALTER USER SA2 SET SALT X'123456' HASH X'78'");

        stmt.executeUpdate("ALTER USER SA2 RENAME TO SA222");
        stmt.executeUpdate("DROP SCHEMA IF EXISTS TEST_SCHEMA2");
        stmt.executeUpdate("ALTER USER SA222 ADMIN false");
        // rightTest();

        byte[] userPasswordHash = SHA256.getKeyPasswordHash("SA222", "test".toCharArray());
        byte[] salt = new byte[Constants.SALT_LEN];
        MathUtils.randomBytes(salt);

        byte[] passwordHash = SHA256.getHashWithSalt(userPasswordHash, salt);

        String passwordHashStr = "X'" + StringUtils.convertBytesToHex(passwordHash) + "'";
        String saltStr = "X'" + StringUtils.convertBytesToHex(salt) + "'";

        stmt.executeUpdate("ALTER USER SA222 SET SALT " + saltStr + " HASH " + passwordHashStr);

        Properties prop = new Properties();
        prop.setProperty("user", "SA222");
        prop.setProperty("password", StringUtils.convertBytesToHex(userPasswordHash));
        prop.setProperty(ConnectionSetting.PASSWORD_HASH.name(), "true");
        Connection conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9092/mydb", prop);
        conn.close();
    }
}
