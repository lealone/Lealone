/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.server.protocol;

public class InitDbPacket extends RequestPacket {

    public String database;

    @Override
    public String getPacketInfo() {
        return "MySQL Init Database Packet";
    }

    @Override
    public void read(PacketInput in) {
        setHeader(in);
        database = in.readStringWithNull();
    }
}
