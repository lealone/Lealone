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

/**
 * From server to client in response to command, if no error and no result set.
 * 
 * <pre>
 * Bytes                       Name
 * -----                       ----
 * 1                           field_count, always = 0
 * 1-9 (Length Coded Binary)   affected_rows
 * 1-9 (Length Coded Binary)   insert_id
 * 2                           server_status
 * 2                           warning_count
 * n   (until end of packet)   message fix:(Length Coded String)
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#OK_Packet
 * </pre>
 * 
 * @author xianmao.hexm 2010-7-16 上午10:33:50
 * @author zhh
 */
public class OkPacket extends ResponsePacket {

    public byte fieldCount = 0x00;
    public long affectedRows;
    public long insertId;
    public int serverStatus;
    public int warningCount;
    public byte[] message;

    @Override
    public String getPacketInfo() {
        return "MySQL OK Packet";
    }

    @Override
    public int calcPacketSize() {
        int i = 1;
        i += getLength(affectedRows);
        i += getLength(insertId);
        i += 4;
        if (message != null) {
            i += getLength(message);
        }
        return i;
    }

    @Override
    public void writeBody(PacketOutput out) {
        out.write(fieldCount);
        out.writeLength(affectedRows);
        out.writeLength(insertId);
        out.writeUB2(serverStatus);
        out.writeUB2(warningCount);
        if (message != null) {
            out.writeWithLength(message);
        }
    }
}
