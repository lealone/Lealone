/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.mysql.server.protocol;

import java.nio.ByteBuffer;

import org.lealone.db.Constants;
import org.lealone.mysql.server.util.BufferUtil;
import org.lealone.mysql.server.util.Capabilities;
import org.lealone.mysql.server.util.CharsetUtil;
import org.lealone.mysql.server.util.RandomUtil;

/**
 * From server to client during initial handshake.
 * 
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 1                            protocol_version
 * n (Null-Terminated String)   server_version
 * 4                            thread_id
 * 8                            scramble_buff
 * 1                            (filler) always 0x00
 * 2                            server_capabilities
 * 1                            server_language
 * 2                            server_status
 * 13                           (filler) always 0x00 ...
 * 13                           rest of scramble_buff (4.1)
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Handshake_Initialization_Packet
 * </pre>
 * 
 * @author xianmao.hexm 2010-7-14 下午05:18:15
 * @author zhh
 */
public class HandshakePacket extends ResponsePacket {

    private static final byte[] FILLER_10 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    private static final byte[] mysql_native_password = "mysql_native_password".getBytes();

    public final byte[] authPluginDataPart2 = RandomUtil.randomBytes(12);

    public byte protocolVersion;
    public byte[] serverVersion;
    public long threadId;
    public byte[] seed;
    public int serverCapabilities;
    public byte serverCharsetIndex;
    public int serverStatus;
    public byte[] restOfScrambleBuff;

    @Override
    public String getPacketInfo() {
        return "MySQL Handshake Packet";
    }

    @Override
    public int calcPacketSize() {
        int size = 1;
        size += serverVersion.length; // n
        size += 5; // 1+4
        size += seed.length; // 8
        size += 19; // 1+2+1+2+13
        size += restOfScrambleBuff.length; // 12
        size += 1; // 1

        size += authPluginDataPart2.length + 1;
        size += mysql_native_password.length + 1;
        return size;
    }

    @Override
    public void writeBody(ByteBuffer buffer, PacketOutput out) {
        buffer.put(protocolVersion);
        BufferUtil.writeWithNull(buffer, serverVersion);
        BufferUtil.writeUB4(buffer, threadId);
        BufferUtil.writeWithNull(buffer, seed);
        BufferUtil.writeUB2(buffer, serverCapabilities);
        buffer.put(serverCharsetIndex);
        BufferUtil.writeUB2(buffer, serverStatus);
        BufferUtil.writeUB2(buffer, Capabilities.CLIENT_PLUGIN_AUTH >> 16);
        buffer.put((byte) (20 + 1));
        buffer.put(FILLER_10);
        BufferUtil.writeWithNull(buffer, authPluginDataPart2);
        BufferUtil.writeWithNull(buffer, mysql_native_password);
        BufferUtil.writeWithNull(buffer, restOfScrambleBuff);
    }

    public static HandshakePacket create(int threadId) {
        // 生成认证数据
        byte[] rand1 = RandomUtil.randomBytes(8);
        byte[] rand2 = RandomUtil.randomBytes(12);

        // 发送握手数据包
        HandshakePacket hs = new HandshakePacket();
        hs.packetId = 0;
        hs.protocolVersion = PROTOCOL_VERSION;
        hs.serverVersion = SERVER_VERSION;
        hs.threadId = threadId;
        hs.seed = rand1;
        hs.serverCapabilities = getServerCapabilities();
        hs.serverCharsetIndex = (byte) (CharsetUtil.getIndex("utf8") & 0xff);
        hs.serverStatus = 2;
        hs.restOfScrambleBuff = rand2;
        return hs;
    }

    /** 协议版本 */
    private static byte PROTOCOL_VERSION = 10;

    /** 服务器版本 */
    private static byte[] SERVER_VERSION = ("5.1.48-" + //
            Constants.PROJECT_NAME + "-" + Constants.RELEASE_VERSION).getBytes();

    private static int getServerCapabilities() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        // flag |= Capabilities.CLIENT_COMPRESS;
        flag |= Capabilities.CLIENT_ODBC;
        // flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= ServerDefs.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        return flag;
    }
}
