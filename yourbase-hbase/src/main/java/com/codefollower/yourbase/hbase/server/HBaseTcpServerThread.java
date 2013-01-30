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
package com.codefollower.yourbase.hbase.server;

import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

import com.codefollower.yourbase.constant.SysProperties;
import com.codefollower.yourbase.engine.ConnectionInfo;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.hbase.engine.HBaseEngine;
import com.codefollower.yourbase.hbase.engine.HBaseSession;
import com.codefollower.yourbase.server.TcpServerThread;
import com.codefollower.yourbase.value.Transfer;

public class HBaseTcpServerThread extends TcpServerThread {
    private HBaseTcpServer server;

    protected HBaseTcpServerThread(Socket socket, HBaseTcpServer server, int id) {
        super(socket, server, id);
        this.server = server;
    }

    @Override
    protected Session createSession(String db, String originalURL, String userName, Transfer transfer) throws IOException {
        byte[] userPasswordHash = transfer.readBytes();
        byte[] filePasswordHash = transfer.readBytes();

        Properties originalProperties = new Properties();

        int len = transfer.readInt();
        String k, v;
        for (int i = 0; i < len; i++) {
            k = transfer.readString();
            v = transfer.readString();
            if (k.equalsIgnoreCase("_userPasswordHash_"))
                userPasswordHash = v.getBytes();
            else if (k.equalsIgnoreCase("_filePasswordHash_"))
                filePasswordHash = v.getBytes();
            else
                originalProperties.setProperty(k, v);

        }
        String baseDir = server.getBaseDir();
        if (baseDir == null) {
            baseDir = SysProperties.getBaseDir();
        }

        String storeEngineName = originalProperties.getProperty("STORE_ENGINE_NAME", null);
        db = server.checkKeyAndGetDatabaseName(db);
        if (storeEngineName != null && storeEngineName.equalsIgnoreCase("HBASE"))
            db = "mem:" + db;
        ConnectionInfo ci = new ConnectionInfo(db);

        if (baseDir != null) {
            ci.setBaseDir(baseDir);
        }
        if (server.getIfExists()) {
            ci.setProperty("IFEXISTS", "TRUE");
        }
        ci.setOriginalURL(originalURL);
        ci.setUserName(userName);

        ci.setUserPasswordHash(userPasswordHash);
        ci.setFilePasswordHash(filePasswordHash);
        ci.readProperties(originalProperties);

        ci.removeProperty("STORE_ENGINE_NAME", false);

        originalProperties.setProperty("user", userName);
        originalProperties.setProperty("password", "");
        if (userPasswordHash != null)
            originalProperties.setProperty("_userPasswordHash_", new String(userPasswordHash));
        if (filePasswordHash != null)
            originalProperties.setProperty("_filePasswordHash_", new String(filePasswordHash));

        if (server.getMaster() != null)
            ci.setProperty("SERVER_TYPE", "M");
        else if (server.getRegionServer() != null)
            ci.setProperty("SERVER_TYPE", "RS");
        HBaseSession session = (HBaseSession) HBaseEngine.getInstance().createSession(ci);
        session.setMaster(server.getMaster());
        session.setRegionServer(server.getRegionServer());
        session.setOriginalProperties(originalProperties);

        return session;
    }
}
