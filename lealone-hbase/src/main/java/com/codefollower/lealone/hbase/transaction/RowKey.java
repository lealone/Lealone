package com.codefollower.lealone.hbase.transaction;

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
        return Bytes.toString(tableId) + ":" + Bytes.toString(rowId);
    }

    public static RowKey readObject(ChannelBuffer aInputStream) {
        RowKey rk = new RowKey(null, null);
        rk.hash = aInputStream.readInt();
        return rk;
    }

    public void writeObject(DataOutputStream aOutputStream) throws IOException {
        hashCode();
        aOutputStream.writeInt(hash);
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
        byte[] key = Arrays.copyOf(tableId, tableId.length + rowId.length);
        System.arraycopy(rowId, 0, key, tableId.length, rowId.length);
        hash = MurmurHash.getInstance().hash(key, 0, key.length, 0xdeadbeef);
        return hash;
    }
}
