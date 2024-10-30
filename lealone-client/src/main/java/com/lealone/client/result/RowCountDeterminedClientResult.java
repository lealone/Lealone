/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.client.result;

import java.io.IOException;

import com.lealone.client.session.ClientSession;
import com.lealone.common.exceptions.DbException;
import com.lealone.db.value.Value;
import com.lealone.net.TransferInputStream;

public class RowCountDeterminedClientResult extends ClientResult {

    public RowCountDeterminedClientResult(ClientSession session, TransferInputStream in, int resultId,
            int columnCount, int rowCount, int fetchSize) throws IOException {
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
                fetchAndReadRows(fetch);
            } else {
                readRows(in, fetch);
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    @Override
    protected void readRows(TransferInputStream in, int fetchSize) throws IOException {
        for (int r = 0; r < fetchSize; r++) {
            if (!in.readBoolean()) {
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
    }
}
