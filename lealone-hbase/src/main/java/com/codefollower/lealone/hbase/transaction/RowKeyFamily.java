package com.codefollower.lealone.hbase.transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;

public class RowKeyFamily extends RowKey {

    private Map<byte[], List<KeyValue>> families;

    public RowKeyFamily() {
        super();
    }

    public RowKeyFamily(byte[] r, byte[] t, Map<byte[], List<KeyValue>> families) {
        super(r, t);
        this.families = families;
    }

    public Map<byte[], List<KeyValue>> getFamilies() {
        return families;
    }

    public void setFamilies(Map<byte[], List<KeyValue>> families) {
        this.families = families;
    }

    public void addFamily(byte[] row, KeyValue kv) {
        List<KeyValue> kvs = families.get(row);
        if (kvs == null) {
            kvs = new ArrayList<KeyValue>();
            families.put(row, kvs);
        }
        kvs.add(kv);
    }

}
