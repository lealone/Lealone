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

import org.lealone.mysql.server.util.Capabilities;

/**
 * From client to server during initial handshake.
 * 
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 4                            client_flags
 * 4                            max_packet_size
 * 1                            charset_number
 * 23                           (filler) always 0x00...
 * n (Null-Terminated String)   user
 * n (Length Coded Binary)      scramble_buff (1 + x bytes)
 * n (Null-Terminated String)   databasename (optional)
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Client_Authentication_Packet
 * </pre>
 * 
 * @author xianmao.hexm 2010-7-15 下午04:35:34
 * @author zhh
 */
public class AuthPacket extends RequestPacket {

    private static final byte[] FILLER = new byte[23];

    public long clientFlags;
    public long maxPacketSize;
    public int charsetIndex;
    public byte[] extra; // from FILLER(23)
    public String user;
    public byte[] password;
    public String database;

    @Override
    public String getPacketInfo() {
        return "MySQL Authentication Packet";
    }

    @Override
    public void read(PacketInput in) {
        super.read(in);
        clientFlags = in.readUB4();
        maxPacketSize = in.readUB4();
        charsetIndex = (in.read() & 0xff);
        // read extra
        int current = in.position();
        int len = (int) in.readLength();
        if (len > 0 && len < FILLER.length) {
            byte[] ab = new byte[len];
            System.arraycopy(in.bytes(), in.position(), ab, 0, len);
            this.extra = ab;
        }
        in.position(current + FILLER.length);
        user = in.readStringWithNull();
        password = in.readBytesWithLength();
        if (((clientFlags & Capabilities.CLIENT_CONNECT_WITH_DB) != 0) && in.hasRemaining()) {
            database = in.readStringWithNull();
        }
    }
}
