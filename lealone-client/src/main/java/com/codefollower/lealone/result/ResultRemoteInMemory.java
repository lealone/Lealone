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

/**
 * The client side part of a result set that is kept on the server.
 * In many cases, the complete data is kept on the client side,
 * but for large results only a subset is in-memory.
 */
public class ResultRemoteInMemory extends ResultRemote {
    public ResultRemoteInMemory(SessionRemote session, Transfer transfer, int id, int columnCount, int rowCount, int fetchSize)
            throws IOException {
        super(session, transfer, id, columnCount, rowCount, fetchSize);
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
        synchronized (session) {
            session.checkClosed();
            try {
                rowOffset += result.size();
                result.clear();
                int fetch = Math.min(fetchSize, rowCount - rowOffset);
                if (sendFetch) {
                    sendFetch(fetch);
                }
                for (int r = 0; r < fetch; r++) {
                    boolean row = transfer.readBoolean();
                    if (!row) {
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
                if (rowOffset + result.size() >= rowCount) {
                    sendClose();
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
        }
    }
}
