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
package org.lealone.server;

public class LealoneDaemon {
    public static void main(String[] args) {
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
