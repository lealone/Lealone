/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MurmurHash;
import org.jboss.netty.buffer.ChannelBuffer;

public class RowKey {
    private byte[] rowId;
    private byte[] tableId;
    private int hash = 0;

    public RowKey() {
        rowId = new byte[0];
        tableId = new byte[0];
    }

    public RowKey(byte[] r, byte[] t) {
        rowId = r;
        tableId = t;
    }

    public byte[] getTable() {
        return tableId;
    }

    public byte[] getRow() {
        return rowId;
    }

    public String toString() {
        return new String(tableId) + ":" + new String(rowId);
    }

    public static RowKey readObject(ChannelBuffer aInputStream) {
        int hash = aInputStream.readInt();
        short len = aInputStream.readByte();
        //      byte[] rowId = RowKeyBuffer.nextRowKey(len);
        byte[] rowId = new byte[len];
        aInputStream.readBytes(rowId, 0, len);
        len = aInputStream.readByte();
        //      byte[] tableId = RowKeyBuffer.nextRowKey(len);
        byte[] tableId = new byte[len];
        aInputStream.readBytes(tableId, 0, len);
        RowKey rk = new RowKey(rowId, tableId);
        rk.hash = hash;
        return rk;
    }

    public void writeObject(DataOutputStream aOutputStream) throws IOException {
        hashCode();
        aOutputStream.writeInt(hash);
        aOutputStream.writeByte(rowId.length);
        aOutputStream.write(rowId, 0, rowId.length);
        aOutputStream.writeByte(tableId.length);
        aOutputStream.write(tableId, 0, tableId.length);
    }

    public boolean equals(Object obj) {
        if (obj instanceof RowKey) {
            RowKey other = (RowKey) obj;

            return Bytes.equals(other.rowId, rowId) && Bytes.equals(other.tableId, tableId);
        }
        return false;
    }

    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        //hash is the xor or row and table id
        /*int h = 0;
        for(int i =0; i < Math.min(8, rowId.length); i++){
          h <<= 8;
          h ^= (int)rowId[i] & 0xFF;
        }
        hash = h;
        h = 0;
        for(int i =0; i < Math.min(8,tableId.length); i++){
          h <<= 8;
          h ^= (int)tableId[i] & 0xFF;
        }
        hash ^= h;
        return hash;*/
        byte[] key = Arrays.copyOf(tableId, tableId.length + rowId.length);
        System.arraycopy(rowId, 0, key, tableId.length, rowId.length);
        hash = MurmurHash.getInstance().hash(key, 0, key.length, 0xdeadbeef);
        //return MurmurHash3.MurmurHash3_x64_32(rowId, 0xDEADBEEF);
        //	    return (31*Arrays.hashCode(tableId)) + Arrays.hashCode(rowId);
        return hash;
    }
}
