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
package org.h2.engine;

import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.h2.result.SimpleRow;
import org.h2.util.HBaseUtils;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueString;

/**
 * 
 * 在HBase中存放一份H2数据库的meta table信息
 *
 */
public class H2MetaTable {

    private final static byte[] TABLE_NAME = Bytes.toBytes("H2_META_TABLE");
    private final static byte[] FAMILY = Bytes.toBytes("info");
    //private final static byte[] ID = Bytes.toBytes("id"); //ID作为rowKey
    private final static byte[] OBJECT_TYPE = Bytes.toBytes("type");
    private final static byte[] SQL = Bytes.toBytes("sql");

    public synchronized static void createTableIfNotExists() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(HBaseUtils.getConfiguration());
        HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);

        HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
        htd.addFamily(hcd);

        if (!admin.tableExists(TABLE_NAME)) {
            admin.createTable(htd);
        }
    }

    public synchronized static void dropTableIfExists() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(HBaseUtils.getConfiguration());
        if (admin.tableExists(TABLE_NAME)) {
            admin.disableTable(TABLE_NAME);
            admin.deleteTable(TABLE_NAME);
        }
    }

    private final HTable table;

    public H2MetaTable() throws Exception {
        createTableIfNotExists();
        table = new HTable(HBaseUtils.getConfiguration(), TABLE_NAME);
    }

    public void loadMetaRecords(List<MetaRecord> records) throws Exception {
        Value[] data;
        for (Result r : table.getScanner(new Scan())) {
            data = new Value[4];
            //id
            //data[0] = ValueInt.get(Bytes.toInt(r.getValue(FAMILY, ID)));
            data[0] = ValueInt.get(Bytes.toInt(r.getRow()));
            //head 未使用
            //data[1] = null;
            //type
            data[2] = ValueInt.get(Bytes.toInt(r.getValue(FAMILY, OBJECT_TYPE)));
            //sql
            data[3] = ValueString.get(Bytes.toString(r.getValue(FAMILY, SQL)));
            records.add(new MetaRecord(new SimpleRow(data)));
        }
    }

    public void addRecord(MetaRecord rec) {
        try {
            Put put = new Put(Bytes.toBytes(rec.getId()));
            put.add(FAMILY, OBJECT_TYPE, Bytes.toBytes(rec.getObjectType()));
            put.add(FAMILY, SQL, Bytes.toBytes(rec.getSQL()));
            table.put(put);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void removeRecord(int id) {
        try {
            Delete delete = new Delete(Bytes.toBytes(id));
            table.delete(delete);
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

    //    private void initHBaseTables(ArrayList<MetaRecord> records) {
    //        try {
    //            HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
    //            HTableDescriptor[] htds = admin.listTables();
    //            if (htds != null && htds.length > 0) {
    //                Value[] data;
    //                for (HTableDescriptor htd : htds) {
    //                    data = new Value[4];
    //                    //id
    //                    data[0] = ValueInt.get(Integer.parseInt(htd.getValue("OBJECT_ID")));
    //                    //type
    //                    data[2] = ValueInt.get(Integer.parseInt(htd.getValue("OBJECT_TYPE")));
    //                    //sql
    //                    data[3] = ValueString.get(HBaseTable.getCreateSQL(htd, htd.getValue("OBJECT_NAME")));
    //
    //                    records.add(new MetaRecord(new SimpleRow(data)));
    //                }
    //            }
    //        } catch (Exception e) {
    //            throw new RuntimeException(e);
    //        }
    //    }
}
