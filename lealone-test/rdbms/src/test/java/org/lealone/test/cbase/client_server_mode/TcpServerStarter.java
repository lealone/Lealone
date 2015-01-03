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
package org.lealone.test.cbase.client_server_mode;

import java.sql.SQLException;
import java.util.ArrayList;

import org.lealone.server.PgServer;
import org.lealone.server.Service;
import org.lealone.server.TcpServer;

public class TcpServerStarter {
    public static void main(String[] args) throws SQLException {

        // System.setProperty("DATABASE_TO_UPPER", "false");
        System.setProperty("lealone.lobInDatabase", "false");
        System.setProperty("lealone.lobClientMaxSizeMemory", "1024");
        System.setProperty("java.io.tmpdir", "./target/test/tmp");
        System.setProperty("lealone.base.dir", "./lealone-test-data/cbase/cs");
        //System.setProperty("lealone.check2", "true");
        ArrayList<String> list = new ArrayList<String>();

        list.add("-tcpPort");
        list.add("5210");

        //list.add("-pg");
        list.add("-tcp");

        start(list.toArray(new String[list.size()]));
    }

    public static void start(String[] args) {
        Service server = null;
        String arg;
        for (int i = 0; args != null && i < args.length; i++) {
            arg = args[i];
            if (arg != null && !arg.isEmpty()) {
                arg = arg.trim();
                switch (arg) {
                case "-tcp":
                    server = new TcpServer();
                    break;
                case "-pg":
                    server = new PgServer();
                    break;
                }
                if (server != null)
                    break;
            }
        }

        if (server == null)
            server = new TcpServer();

        try {
            server.init(args);
            server.start();
            System.out.println("Lealone daemon started, listening tcp port: " + server.getPort());
            server.listen();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
