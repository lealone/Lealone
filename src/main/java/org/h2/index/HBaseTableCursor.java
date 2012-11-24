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
package org.h2.index;

import java.io.IOException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.h2.value.ValueString;

public class HBaseTableCursor implements Cursor {
    private final Session session;
    private long scannerId;
    private Result[] result;
    private int length = 0;

    public HBaseTableCursor(Session session, SearchRow first, SearchRow last) {
        this.session = session;
        try {
            Scan scan = new Scan();
            //            if (first != null)
            //                scan.setStartRow(Bytes.toBytes(Long.toString(first.getKey())));
            //            if (last != null)
            //                scan.setStopRow(Bytes.toBytes(Long.toString(last.getKey())));
            HRegionInfo info = session.getRegionServer().getRegionInfo(session.getRegionName());
            scan.setStartRow(info.getStartKey());
            scan.setStopRow(info.getEncodedNameAsBytes());
            scannerId = session.getRegionServer().openScanner(session.getRegionName(), scan);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Row get() {
        if (result == null || length >= result.length) {
            try {
                result = session.getRegionServer().next(scannerId, session.getFetchSize());
                length = 0;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (result != null && length < result.length) {
            Result r = result[length++];
            KeyValue[] kvs = r.raw();
            int size = kvs.length;
            Value[] data = new Value[size];

            for (int i = 0; i < size; i++) {
                data[i] = ValueString.get(Bytes.toString(kvs[i].getValue()));
            }
            return new Row(data, Row.MEMORY_CALCULATE);
        }
        return null;
    }

    @Override
    public SearchRow getSearchRow() {
        return get();
    }

    @Override
    public boolean next() {
        if (result != null && length < result.length)
            return true;

        try {
            result = session.getRegionServer().next(scannerId, session.getFetchSize());
            length = 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (result != null && result.length > 0)
            return true;

        return false;
    }

    @Override
    public boolean previous() {
        return false;
    }

}
