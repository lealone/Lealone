/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mysql.server.protocol;

public class InitDbPacket extends RequestPacket {

    public String database;

    @Override
    public String getPacketInfo() {
        return "MySQL Init Database Packet";
    }

    @Override
    public void read(PacketInput in) {
        super.read(in);
        database = in.readStringWithNull();
    }
}
