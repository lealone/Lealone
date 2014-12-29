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
package org.lealone.hbase.server;

import java.net.Socket;
import java.sql.SQLException;
import java.util.Properties;

import org.lealone.engine.ConnectionInfo;
import org.lealone.engine.Constants;
import org.lealone.engine.SysProperties;
import org.lealone.hbase.engine.HBaseConnectionInfo;
import org.lealone.hbase.engine.HBaseDatabaseEngine;
import org.lealone.hbase.engine.HBaseSession;
import org.lealone.hbase.zookeeper.ZooKeeperAdmin;
import org.lealone.jdbc.JdbcConnection;
import org.lealone.security.SHA256;
import org.lealone.server.PgServerThread;
import org.lealone.util.StringUtils;

public class HBasePgServerThread extends PgServerThread {
    private final HBasePgServer server;

    protected HBasePgServerThread(Socket socket, HBasePgServer server) {
        super(socket, server);
        this.server = server;
    }

    @Override
    protected JdbcConnection createJdbcConnection(String password) throws SQLException {
        byte[] userPasswordHash = hashPassword(StringUtils.toUpperEnglish(userName), password.toCharArray());
        Properties originalProperties = new Properties();
        originalProperties.put("MODE", "PostgreSQL");
        originalProperties.put("USER", userName);
        originalProperties.put("_userPasswordHash_", userPasswordHash);

        String baseDir = server.getBaseDir();
        if (baseDir == null) {
            baseDir = SysProperties.getBaseDir();
        }

        String host;
        int port;
        if (server.getMaster() != null) {
            host = server.getMaster().getServerName().getHostname();
            port = ZooKeeperAdmin.getTcpPort(server.getMaster().getServerName());
        } else {
            host = server.getRegionServer().getServerName().getHostname();
            port = ZooKeeperAdmin.getTcpPort(server.getRegionServer().getServerName());
        }
        String url = Constants.URL_PREFIX + Constants.URL_TCP + "//" + host + ":" + port + "/" + databaseName;
        ConnectionInfo ci = new HBaseConnectionInfo(url, databaseName);

        if (baseDir != null) {
            ci.setBaseDir(baseDir);
        }
        if (server.getIfExists()) {
            ci.setProperty("IFEXISTS", "TRUE");
        }

        ci.setUserName(userName);
        ci.setProperty("MODE", "PostgreSQL");

        ci.setUserPasswordHash(userPasswordHash);
        ci.setFilePasswordHash(null);

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
