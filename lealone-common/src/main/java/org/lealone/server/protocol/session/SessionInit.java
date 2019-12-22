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
package org.lealone.server.protocol.session;

import java.io.IOException;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.api.ErrorCode;
import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class SessionInit implements Packet {

    public final ConnectionInfo ci;
    public final int clientVersion;

    public SessionInit(ConnectionInfo ci) {
        this.ci = ci;
        this.clientVersion = 0;
    }

    public SessionInit(ConnectionInfo ci, int clientVersion) {
        this.ci = ci;
        this.clientVersion = clientVersion;
    }

    @Override
    public PacketType getType() {
        return PacketType.SESSION_INIT;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.SESSION_INIT_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.setSSL(ci.isSSL());
        out.writeInt(Constants.TCP_PROTOCOL_VERSION_1); // minClientVersion
        out.writeInt(Constants.TCP_PROTOCOL_VERSION_1); // maxClientVersion
        out.writeString(ci.getDatabaseShortName());
        out.writeString(ci.getURL()); // 不带参数的URL
        out.writeString(ci.getUserName());
        out.writeBytes(ci.getUserPasswordHash());
        out.writeBytes(ci.getFilePasswordHash());
        out.writeBytes(ci.getFileEncryptionKey());
        String[] keys = ci.getKeys();
        out.writeInt(keys.length);
        for (String key : keys) {
            out.writeString(key).writeString(ci.getProperty(key));
        }
    }

    public static final PacketDecoder<SessionInit> decoder = new Decoder();

    private static class Decoder implements PacketDecoder<SessionInit> {
        @Override
        public SessionInit decode(NetInputStream in, int version) throws IOException {
            int minClientVersion = in.readInt();
            if (minClientVersion < Constants.TCP_PROTOCOL_VERSION_MIN) {
                throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2, "" + minClientVersion,
                        "" + Constants.TCP_PROTOCOL_VERSION_MIN);
            } else if (minClientVersion > Constants.TCP_PROTOCOL_VERSION_MAX) {
                throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2, "" + minClientVersion,
                        "" + Constants.TCP_PROTOCOL_VERSION_MAX);
            }
            int clientVersion;
            int maxClientVersion = in.readInt();
            if (maxClientVersion >= Constants.TCP_PROTOCOL_VERSION_MAX) {
                clientVersion = Constants.TCP_PROTOCOL_VERSION_CURRENT;
            } else {
                clientVersion = minClientVersion;
            }
            ConnectionInfo ci = createConnectionInfo(in);
            return new SessionInit(ci, clientVersion);
        }

        private ConnectionInfo createConnectionInfo(NetInputStream in) throws IOException {
            String dbName = in.readString();
            String originalURL = in.readString();
            String userName = in.readString();
            ConnectionInfo ci = new ConnectionInfo(originalURL, dbName);

            ci.setUserName(userName);
            ci.setUserPasswordHash(in.readBytes());
            ci.setFilePasswordHash(in.readBytes());
            ci.setFileEncryptionKey(in.readBytes());

            int len = in.readInt();
            for (int i = 0; i < len; i++) {
                String key = in.readString();
                String value = in.readString();
                ci.addProperty(key, value, true); // 一些不严谨的client driver可能会发送重复的属性名
            }
            ci.initTraceProperty();
            return ci;
        }
    }
}
