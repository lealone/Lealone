/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.server.protocol;

import org.lealone.plugins.mysql.server.MySQLServer;
import org.lealone.plugins.mysql.server.util.Capabilities;
import org.lealone.plugins.mysql.server.util.CharsetUtil;
import org.lealone.plugins.mysql.server.util.RandomUtil;

// server发给client的第一个握手包
// 包格式参考: https://dev.mysql.com/doc/dev/mysql-server/latest/
// page_protocol_connection_phase_packets_protocol_handshake_v10.html
public class HandshakePacket extends ResponsePacket {

    private static final byte[] FILLER_10 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    private static final byte[] MYSQL_NATIVE_PASSWORD = "mysql_native_password".getBytes();

    // 协议版本，Always 10
    private static final byte PROTOCOL_VERSION = 10;

    // 服务器版本
    private static final byte[] SERVER_VERSION = MySQLServer.SERVER_VERSION.getBytes();

    private byte protocolVersion;
    private byte[] serverVersion;
    private long threadId;
    private byte[] seed;
    private int serverCapabilities;
    private byte serverCharsetIndex;
    private int serverStatus;
    private byte[] authPluginDataPart2;
    private byte[] restOfScrambleBuff;

    public HandshakePacket(int tid) {
        // 生成认证数据
        byte[] rand1 = RandomUtil.randomBytes(8);
        byte[] rand2 = RandomUtil.randomBytes(12);

        packetId = 0;
        protocolVersion = PROTOCOL_VERSION;
        serverVersion = SERVER_VERSION;
        threadId = tid;
        seed = rand1;
        serverCapabilities = Capabilities.getServerCapabilities();
        serverCharsetIndex = (byte) (CharsetUtil.getIndex("utf8") & 0xff);
        serverStatus = 2;
        authPluginDataPart2 = RandomUtil.randomBytes(12);
        restOfScrambleBuff = rand2;
    }

    public byte[] getSalt() {
        // 不能用restOfScrambleBuff
        byte[] salt = new byte[seed.length + authPluginDataPart2.length];
        System.arraycopy(seed, 0, salt, 0, seed.length);
        System.arraycopy(authPluginDataPart2, 0, salt, seed.length, authPluginDataPart2.length);
        return salt;
    }

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
        size += MYSQL_NATIVE_PASSWORD.length + 1;
        return size;
    }

    @Override
    public void writeBody(PacketOutput out) {
        out.write(protocolVersion);
        out.writeWithNull(serverVersion);
        out.writeUB4(threadId);
        out.writeWithNull(seed);
        out.writeUB2(serverCapabilities);
        out.write(serverCharsetIndex);
        out.writeUB2(serverStatus);
        out.writeUB2(Capabilities.CLIENT_PLUGIN_AUTH >> 16);
        out.write((byte) (20 + 1));
        out.write(FILLER_10);
        out.writeWithNull(authPluginDataPart2);
        out.writeWithNull(MYSQL_NATIVE_PASSWORD);
        out.writeWithNull(restOfScrambleBuff);
    }
}
