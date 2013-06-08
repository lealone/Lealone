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
package com.codefollower.lealone.hbase.server;

import java.net.Socket;
import java.sql.SQLException;
import java.util.Properties;

import com.codefollower.lealone.constant.SysProperties;
import com.codefollower.lealone.engine.ConnectionInfo;
import com.codefollower.lealone.hbase.engine.HBaseDatabaseEngine;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.jdbc.JdbcConnection;
import com.codefollower.lealone.security.SHA256;
import com.codefollower.lealone.server.pg.PgServerThread;

public class HBasePgServerThread extends PgServerThread {
    private HBasePgServer server;

    protected HBasePgServerThread(Socket socket, HBasePgServer server) {
        super(socket, server);
        this.server = server;
    }

//    @Override
//    protected JdbcConnection createJdbcConnection(String password) throws SQLException {
//        Properties info = new Properties();
//        info.put("MODE", "PostgreSQL");
//        info.put("USER", userName);
//        info.put("PASSWORD", password);
//        String url = "jdbc:lealone:" + databaseName;
//        ConnectionInfo ci = new ConnectionInfo(url, info);
//        String baseDir = server.getBaseDir();
//        if (baseDir == null) {
//            baseDir = SysProperties.getBaseDir();
//        }
//        if (baseDir != null) {
//            ci.setBaseDir(baseDir);
//        }
//        if (server.getIfExists()) {
//            ci.setProperty("IFEXISTS", "TRUE");
//        }
//
//        return new JdbcConnection(ci, false);
//    }

    @Override
    protected JdbcConnection createJdbcConnection(String password) throws SQLException {
        byte[] userPasswordHash = hashPassword(userName, password.toCharArray());
        byte[] filePasswordHash = null;

        Properties originalProperties = new Properties();

        //        int len = dataIn.readInt();
        //        String k, v;
        //        for (int i = 0; i < len; i++) {
        //            k = dataIn.readUTF();
        //            v = dataIn.readUTF();
        //            if (k.equalsIgnoreCase("_userPasswordHash_"))
        //                userPasswordHash = v.getBytes();
        //            else if (k.equalsIgnoreCase("_filePasswordHash_"))
        //                filePasswordHash = v.getBytes();
        //            else
        //                originalProperties.setProperty(k, v);
        //
        //        }

        originalProperties.put("MODE", "PostgreSQL");
        //        originalProperties.put("USER", userName);
        //        originalProperties.put("PASSWORD", password);

        String baseDir = server.getBaseDir();
        if (baseDir == null) {
            baseDir = SysProperties.getBaseDir();
        }

        databaseName = "mem:" + databaseName; //TODO

        ConnectionInfo ci = new ConnectionInfo(databaseName);

        if (baseDir != null) {
            ci.setBaseDir(baseDir);
        }
        if (server.getIfExists()) {
            ci.setProperty("IFEXISTS", "TRUE");
        }

        if (server.getMaster() != null)
            ci.setOriginalURL("jdbc:lealone:" + server.getMaster().getServerName().getHostAndPort() + "/" + databaseName);
        else
            ci.setOriginalURL("jdbc:lealone:" //
                    + server.getRegionServer().getServerName().getHostAndPort() + "/" + databaseName);
        ci.setUserName(userName);

        ci.setUserPasswordHash(userPasswordHash);
        ci.setFilePasswordHash(filePasswordHash);
        ci.readProperties(originalProperties);

        originalProperties.setProperty("user", userName);
        originalProperties.setProperty("password", "");
        if (userPasswordHash != null)
            originalProperties.setProperty("_userPasswordHash_", new String(userPasswordHash));
        //if (filePasswordHash != null)
        //    originalProperties.setProperty("_filePasswordHash_", new String(filePasswordHash));

        if (server.getMaster() != null)
            ci.setProperty("SERVER_TYPE", "M");
        else if (server.getRegionServer() != null)
            ci.setProperty("SERVER_TYPE", "RS");
        HBaseSession session = (HBaseSession) HBaseDatabaseEngine.getInstance().createSession(ci);
        session.setMaster(server.getMaster());
        session.setRegionServer(server.getRegionServer());
        session.setOriginalProperties(originalProperties);
        ci.setSession(session);

        return new JdbcConnection(ci, false);
    }

    private static byte[] hashPassword(String userName, char[] password) {
        if (userName.length() == 0 && password.length == 0) {
            return new byte[0];
        }
        return SHA256.getKeyPasswordHash(userName, password);
    }

}
