/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.result;

import java.io.IOException;

import com.codefollower.lealone.engine.SessionRemote;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.value.Transfer;
import com.codefollower.lealone.value.Value;

public class ResultRemoteCursor extends ResultRemote {
    //不能在这初始化为false，在super的构造函数中会调用fetchRows有可能把isEnd设为true了，
    //如果初始化为false，相当于在调用完super(...)后再执行isEnd = false，这时前面的值就被覆盖了。
    private boolean isEnd;

    public ResultRemoteCursor(SessionRemote session, Transfer transfer, int id, int columnCount, int fetchSize)
            throws IOException {
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
                if (isEnd && result.size() == 0) {
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
        return Integer.MAX_VALUE; //不能返回-1，JdbcResultSet那边会抛异常
    }

    @Override
    protected void fetchRows(boolean sendFetch) {
        synchronized (session) {
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

}
