/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.lealone.cassandra.jdbc.dml;

import org.junit.Test;
import org.lealone.cassandra.jdbc.TestBase;

public class InsertTest extends TestBase {
    @Test
    public void run() throws Exception {

        tableName = "InsertTest";
        
//        create();
//        insert();
        

        
        sql = "SELECT * FROM " + tableName + " WHERE block_id = 3";
        printResultSet();
    }

    void create() throws Exception {
        sql = "CREATE TABLE IF NOT EXISTS " + tableName //
                + " ( block_id int, short_hair boolean, f1 text, " //
                + "PRIMARY KEY (block_id, short_hair)) WITH compaction = { 'class' : 'LeveledCompactionStrategy'}";

        executeUpdate();
    }

    void insert() throws Exception {
        int count = 4;
        for (int i = 0; i < count; i++) {
            sql = "INSERT INTO " + tableName + "(block_id, short_hair, f1) VALUES (" + i + ", true, (text)'ab" + i + "')";
            executeUpdate();
        }
    }
}
