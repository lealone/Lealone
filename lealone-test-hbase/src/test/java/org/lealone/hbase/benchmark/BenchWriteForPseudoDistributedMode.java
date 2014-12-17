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
package org.lealone.hbase.benchmark;

public class BenchWriteForPseudoDistributedMode extends BenchWrite {

    public static void main(String[] args) throws Exception {
        new BenchWriteForPseudoDistributedMode(10000, 10003).run();
        new BenchWriteForPseudoDistributedMode(10000, 10500).run();
    }

    //endKey-startKey的值就是一个事务中包含的写操作个数
    public BenchWriteForPseudoDistributedMode(int startKey, int endKey) {
        super("BenchWriteForPseudoDistributedMode", startKey, endKey);
    }

    public void createTable() throws Exception {
        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS " + tableName + " (" //
                + "SPLIT KEYS('RK10300'), " //预分region
                + "COLUMN FAMILY cf(id int, name varchar(500), age long, salary double))");
    }
}
