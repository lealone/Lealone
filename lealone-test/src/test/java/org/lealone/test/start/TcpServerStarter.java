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
package org.lealone.test.start;

import java.sql.SQLException;
import java.util.ArrayList;

import org.lealone.server.LealoneDaemon;

public class TcpServerStarter {
    public static void main(String[] args) throws SQLException {

        // System.setProperty("DATABASE_TO_UPPER", "false");
        System.setProperty("lealone.lobInDatabase", "false");
        System.setProperty("lealone.lobClientMaxSizeMemory", "1024");
        System.setProperty("java.io.tmpdir", "./target/test/tmp");
        System.setProperty("lealone.baseDir", "./target/test/baseDir");
        //System.setProperty("lealone.check2", "true");
        ArrayList<String> list = new ArrayList<String>();

        list.add("-tcpPort");
        list.add("9092");

        //list.add("-pg");
        list.add("-tcp");

        LealoneDaemon.main(list.toArray(new String[list.size()]));
    }
}
