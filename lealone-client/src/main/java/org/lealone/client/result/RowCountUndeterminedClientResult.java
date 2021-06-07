/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.client.result;

import java.io.IOException;

import org.lealone.client.session.ClientSession;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.Value;
import org.lealone.net.TransferInputStream;

public class RowCountUndeterminedClientResult extends ClientResult {

    // 不能在这初始化为false，在super的构造函数中会调用fetchRows有可能把isEnd设为true了，
    // 如果初始化为false，相当于在调用完super(...)后再执行isEnd = false，这时前面的值就被覆盖了。
    private boolean isEnd;

    public RowCountUndeterminedClientResult(ClientSession session, TransferInputStream in, int resultId,
            int columnCount, int fetchSize) throws IOException {
        super(session, in, resultId, columnCount, -1, fetchSize);
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
                boolean row = in.readBoolean();
                if (!row) {
                    isEnd = true;
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

            if (isEnd)
                sendClose();
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }
}
