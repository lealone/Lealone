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
package org.lealone.hbase.jdbc.transaction;

import static junit.framework.Assert.assertEquals;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.lealone.hbase.jdbc.TestBase;
import org.lealone.hbase.util.HBaseUtils;

//测试当数据被切分后被移到了不同的region server仍然能识别事务的有效性
public class SplitTest extends TestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS SplitTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS SplitTest (id int primary key, name varchar(500))");

        try {
            conn.setAutoCommit(false);
            for (int i = 1; i <= 50; i++)
                stmt.executeUpdate("INSERT INTO SplitTest(id, name) VALUES(" + i + ", 'a1')");
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }

        sql = "SELECT count(*) FROM SplitTest";
        assertEquals(50, getIntValue(1, true));

        String tableName = "SplitTest".toUpperCase();
        printRegions(tableName);

        HBaseAdmin admin = HBaseUtils.getHBaseAdmin();
        admin.split(Bytes.toBytes(tableName), Bytes.toBytes(20));
        Thread.sleep(2000);

        admin.split(Bytes.toBytes(tableName), Bytes.toBytes(40));
        Thread.sleep(2000);

        admin.balancer();
        Thread.sleep(2000);

        printRegions(tableName);

        sql = "SELECT count(*) FROM SplitTest";
        assertEquals(50, getIntValue(1, true));
    }
}
