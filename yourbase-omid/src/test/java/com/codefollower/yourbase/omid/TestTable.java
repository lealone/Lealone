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

package com.codefollower.yourbase.omid;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class TestTable {
    private static final Log LOG = LogFactory.getLog(TestTable.class);

    private static final String TEST_TABLE = "usertable";
    private static final String TEST_FAMILY = "Teste";

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(TEST_TABLE)) {
            HTableDescriptor desc = new HTableDescriptor(TEST_TABLE);
            HColumnDescriptor column = new HColumnDescriptor(TEST_FAMILY);
            desc.addFamily(column);

            //         HColumnDescriptor delfam = new HColumnDescriptor(TransactionalTable.DELETE_STATUS_FAMILY);
            //         delfam.setMaxVersions(10);
            //         desc.addFamily(delfam);
            admin.createTable(desc);
            LOG.info("created table");
        }
    }
}