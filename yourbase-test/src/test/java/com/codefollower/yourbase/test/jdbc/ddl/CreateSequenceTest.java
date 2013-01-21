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
package com.codefollower.yourbase.test.jdbc.ddl;

import org.junit.Test;

import com.codefollower.yourbase.test.jdbc.TestBase;

public class CreateSequenceTest extends TestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("DROP SEQUENCE IF EXISTS myseq");
        //stmt.executeUpdate("CREATE SEQUENCE IF NOT EXISTS myseq START WITH 1000 INCREMENT BY 1 CACHE 20 BELONGS_TO_TABLE");
        stmt.executeUpdate("CREATE SEQUENCE IF NOT EXISTS myseq START WITH 1000 INCREMENT BY 1 CACHE 20");

        sql = "select myseq.CURRVAL, myseq.NEXTVAL";
        printResultSet();
    }
}
