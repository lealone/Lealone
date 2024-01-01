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
package com.lealone.plugins.mysql.server.protocol;

/**
 * From Server To Client, part of Result Set Packets. One for each column in the
 * result set. Thus, if the value of field_columns in the Result Set Header
 * Packet is 3, then the Field Packet occurs 3 times.
 * 
 * <pre>
 * Bytes                      Name
 * -----                      ----
 * n (Length Coded String)    catalog
 * n (Length Coded String)    db
 * n (Length Coded String)    table
 * n (Length Coded String)    org_table
 * n (Length Coded String)    name
 * n (Length Coded String)    org_name
 * 1                          (filler)
 * 2                          charsetNumber
 * 4                          length
 * 1                          type
 * 2                          flags
 * 1                          decimals
 * 2                          (filler), always 0x00
 * n (Length Coded Binary)    default
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Field_Packet
 * </pre>
 * 
 * @author xianmao.hexm 2010-7-22 下午05:43:34
 * @author zhh
 */
public class FieldPacket extends ResponsePacket {

    private static final byte[] DEFAULT_CATALOG = "def".getBytes();
    private static final byte[] FILLER = new byte[2];

    public byte[] catalog = DEFAULT_CATALOG;
    public byte[] db;
    public byte[] table;
    public byte[] orgTable;
    public byte[] name;
    public byte[] orgName;
    public int charsetIndex;
    public long length;
    public int type;
    public int flags;
    public byte decimals;
    public byte[] definition;

    @Override
    public String getPacketInfo() {
        return "MySQL Field Packet";
    }

    @Override
    public int calcPacketSize() {
        int size = (catalog == null ? 1 : getLength(catalog));
        size += (db == null ? 1 : getLength(db));
        size += (table == null ? 1 : getLength(table));
        size += (orgTable == null ? 1 : getLength(orgTable));
        size += (name == null ? 1 : getLength(name));
        size += (orgName == null ? 1 : getLength(orgName));
        size += 13; // 1+2+4+1+2+1+2
        if (definition != null) {
            size += getLength(definition);
        }
        return size;
    }

    @Override
    public void writeBody(PacketOutput out) {
        byte nullVal = 0;
        out.writeWithLength(catalog, nullVal);
        out.writeWithLength(db, nullVal);
        out.writeWithLength(table, nullVal);
        out.writeWithLength(orgTable, nullVal);
        out.writeWithLength(name, nullVal);
        out.writeWithLength(orgName, nullVal);
        out.write((byte) 0x0C);
        out.writeUB2(charsetIndex);
        out.writeUB4(length);
        out.write((byte) (type & 0xff));
        out.writeUB2(flags);
        out.write(decimals);
        out.getBuffer().position(out.getBuffer().position() + FILLER.length);
        if (definition != null) {
            out.writeWithLength(definition);
        }
    }
}
