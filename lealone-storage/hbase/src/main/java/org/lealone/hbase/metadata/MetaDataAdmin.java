/*
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
package org.lealone.hbase.metadata;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.lealone.engine.Constants;
import org.lealone.hbase.util.HBaseUtils;

public class MetaDataAdmin {
    public final static String META_DATA_PREFIX = Constants.PROJECT_NAME + "_";
    public final static byte[] DEFAULT_COLUMN_FAMILY = Bytes.toBytes("CF");

    public synchronized static void createTableIfNotExists(byte[] tableName) throws IOException {
        createTableIfNotExists(tableName, DEFAULT_COLUMN_FAMILY);
    }

    public synchronized static void createTableIfNotExists(byte[] tableName, byte[] family) throws IOException {
        HBaseAdmin admin = HBaseUtils.getHBaseAdmin();
        if (!admin.tableExists(tableName)) {
            HColumnDescriptor hcd = new HColumnDescriptor(family);
            hcd.setMaxVersions(1); //只需要保留最新版本即可

            HTableDescriptor htd = new HTableDescriptor(tableName);
            htd.addFamily(hcd);
            admin.createTable(htd);
        }
    }

    public synchronized static void dropTableIfExists(byte[] tableName) throws IOException {
        HBaseAdmin admin = HBaseUtils.getHBaseAdmin();
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }
}
