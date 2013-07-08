/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.codefollower.lealone.hbase.metadata;

import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import com.codefollower.lealone.engine.MetaRecord;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.result.SimpleRow;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueInt;
import com.codefollower.lealone.value.ValueString;

/**
 * 
 * 在HBase中存放数据库的元数据(比如create sql、alter sql等)
 * 
 * 此类不是线程安全的，只在Master端使用。
 *
 */
public class MetaDataTable {
    private final static byte[] TABLE_NAME = Bytes.toBytes(MetaDataAdmin.META_DATA_PREFIX + "meta_data_table");
    //private final static byte[] ID = Bytes.toBytes("id");
    private final static byte[] OBJECT_TYPE = Bytes.toBytes("type");
    private final static byte[] SQL = Bytes.toBytes("sql");

    private final HTable table;

    public MetaDataTable() throws Exception {
        MetaDataAdmin.createTableIfNotExists(TABLE_NAME);
        table = new HTable(HBaseUtils.getConfiguration(), TABLE_NAME);
    }

    public void loadMetaRecords(List<MetaRecord> records) throws Exception {
        MetaRecord rec;
        for (Result r : table.getScanner(new Scan())) {
            if (r.isEmpty())
                continue;
            rec = getMetaRecord(r);
            records.add(rec);
        }
    }

    public void addRecord(MetaRecord rec) {
        try {
            addMetaRecord(rec);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void removeRecord(int id) {
        try {
            MetaRecord rec = getMetaRecord(id);
            if (rec != null) {
                table.delete(new Delete(Bytes.toBytes(id)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void updateRecord(MetaRecord rec) {
        try {
            addRecord(rec);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void addMetaRecord(MetaRecord rec) throws Exception {
        Put put = new Put(Bytes.toBytes(rec.getId()));
        put.add(MetaDataAdmin.DEFAULT_FAMILY, OBJECT_TYPE, Bytes.toBytes(rec.getObjectType()));
        put.add(MetaDataAdmin.DEFAULT_FAMILY, SQL, Bytes.toBytes(rec.getSQL()));
        table.put(put);
    }

    private MetaRecord getMetaRecord(Result r) {
        Value[] data = new Value[4];
        //id
        //data[0] = ValueInt.get(Bytes.toInt(r.getValue(MetaDataAdmin.DEFAULT_FAMILY, ID)));
        data[0] = ValueInt.get(Bytes.toInt(r.getRow()));
        //head 未使用
        //data[1] = null;
        //type
        data[2] = ValueInt.get(Bytes.toInt(r.getValue(MetaDataAdmin.DEFAULT_FAMILY, OBJECT_TYPE)));
        //sql
        data[3] = ValueString.get(Bytes.toString(r.getValue(MetaDataAdmin.DEFAULT_FAMILY, SQL)));
        return new MetaRecord(new SimpleRow(data));

    }

    private MetaRecord getMetaRecord(int id) {
        try {
            Result r = table.get(new Get(Bytes.toBytes(id)));
            if (!r.isEmpty())
                return getMetaRecord(r);
            else
                return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (table != null) {
            try {
                table.close();
            } catch (Exception e) {
                //ignore
            }
        }
    }
}
