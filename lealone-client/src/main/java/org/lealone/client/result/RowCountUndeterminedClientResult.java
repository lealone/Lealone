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
package org.lealone.client.result;

import java.io.IOException;

import org.lealone.client.ClientSession;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.Value;
import org.lealone.net.Transfer;

public class RowCountUndeterminedClientResult extends ClientResult {

    // 不能在这初始化为false，在super的构造函数中会调用fetchRows有可能把isEnd设为true了，
    // 如果初始化为false，相当于在调用完super(...)后再执行isEnd = false，这时前面的值就被覆盖了。
    private boolean isEnd;

    public RowCountUndeterminedClientResult(ClientSession session, Transfer transfer, int id, int columnCount,
            int fetchSize) throws IOException {
        super(session, transfer, id, columnCount, -1, fetchSize);
    }

    @Override
    public boolean next() {
        if (isEnd && rowId - rowOffset >= result.size() - 1) {
            currentRow = null;
            return false;
        }

        rowId++;
        if (!isEnd) {
            remapIfOld();
            if (rowId - rowOffset >= result.size()) {
                fetchRows(true);
                if (isEnd && result.isEmpty()) {
                    currentRow = null;
                    return false;
                }
            }
        }
        currentRow = result.get(rowId - rowOffset);
        return true;

    }

    @Override
    public int getRowCount() {
        return Integer.MAX_VALUE; // 不能返回-1，JdbcResultSet那边会抛异常
    }

    @Override
    protected void fetchRows(boolean sendFetch) {
        session.checkClosed();
        try {
            rowOffset += result.size();
            result.clear();
            if (sendFetch) {
                sendFetch(fetchSize);
            }
            for (int r = 0; r < fetchSize; r++) {
                boolean row = transfer.readBoolean();
                if (!row) {
                    isEnd = true;
                    break;
                }
                int len = columns.length;
                Value[] values = new Value[len];
                for (int i = 0; i < len; i++) {
                    Value v = transfer.readValue();
                    values[i] = v;
                }
                result.add(values);
            }

            if (isEnd)
                sendClose();
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

}
