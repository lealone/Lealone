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
import org.lealone.net.TransferInputStream;

public class RowCountDeterminedClientResult extends ClientResult {

    public RowCountDeterminedClientResult(ClientSession session, TransferInputStream in, int resultId, int columnCount,
            int rowCount, int fetchSize) throws IOException {
        super(session, in, resultId, columnCount, rowCount, fetchSize);
    }

    @Override
    public boolean next() {
        if (rowId < rowCount) {
            rowId++;
            remapIfOld();
            if (rowId < rowCount) {
                if (rowId - rowOffset >= result.size()) {
                    fetchRows(true);
                }
                currentRow = result.get(rowId - rowOffset);
                return true;
            }
            currentRow = null;
        }
        return false;
    }

    @Override
    protected void fetchRows(boolean sendFetch) {
        session.checkClosed();
        try {
            rowOffset += result.size();
            result.clear();
            int fetch = Math.min(fetchSize, rowCount - rowOffset);
            if (sendFetch) {
                sendFetch(fetch);
            }
            for (int r = 0; r < fetch; r++) {
                boolean row = in.readBoolean();
                if (!row) {
                    break;
                }
                int len = columns.length;
                Value[] values = new Value[len];
                for (int i = 0; i < len; i++) {
                    Value v = in.readValue();
                    values[i] = v;
                }
                result.add(values);
            }
            if (rowOffset + result.size() >= rowCount) {
                sendClose();
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }
}
