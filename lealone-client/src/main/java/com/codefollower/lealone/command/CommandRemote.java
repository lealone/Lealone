/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.command;

import java.io.IOException;
import java.util.ArrayList;

import com.codefollower.lealone.constant.SysProperties;
import com.codefollower.lealone.engine.SessionRemote;
import com.codefollower.lealone.expression.ParameterInterface;
import com.codefollower.lealone.expression.ParameterRemote;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.message.Trace;
import com.codefollower.lealone.result.ResultInterface;
import com.codefollower.lealone.result.ResultRemote;
import com.codefollower.lealone.result.ResultRemoteCursor;
import com.codefollower.lealone.result.ResultRemoteInMemory;
import com.codefollower.lealone.util.New;
import com.codefollower.lealone.value.Transfer;
import com.codefollower.lealone.value.Value;

/**
 * Represents the client-side part of a SQL statement.
 * This class is not used in embedded mode.
 */
public class CommandRemote implements CommandInterface {

    private final ArrayList<Transfer> transferList;
    private final ArrayList<ParameterInterface> parameters;
    private final Trace trace;
    private final String sql;
    private int fetchSize;
    private SessionRemote session;
    private int id;
    private boolean isQuery;
    private boolean readonly;
    private final int created;

    private byte[][] transactionalRowKeys;
    private Long startTimestamp;

    public CommandRemote(SessionRemote session, ArrayList<Transfer> transferList, String sql, int fetchSize) {
        this.transferList = transferList;
        trace = session.getTrace();
        this.sql = sql;
        parameters = New.arrayList();
        prepare(session, true);
        // set session late because prepare might fail - in this case we don't
        // need to close the object
        this.session = session;
        this.fetchSize = fetchSize;
        created = session.getLastReconnect();
    }

    private void prepare(SessionRemote s, boolean createParams) {
        id = s.getNextId();
        for (int i = 0, count = 0; i < transferList.size(); i++) {
            try {
                Transfer transfer = transferList.get(i);
                if (createParams) {
                    s.traceOperation("SESSION_PREPARE_READ_PARAMS", id);
                    transfer.writeInt(SessionRemote.SESSION_PREPARE_READ_PARAMS).writeInt(id).writeString(sql);
                } else {
                    s.traceOperation("SESSION_PREPARE", id);
                    transfer.writeInt(SessionRemote.SESSION_PREPARE).writeInt(id).writeString(sql);
                }
                s.done(transfer);
                isQuery = transfer.readBoolean();
                readonly = transfer.readBoolean();
                int paramCount = transfer.readInt();
                if (createParams) {
                    parameters.clear();
                    for (int j = 0; j < paramCount; j++) {
                        ParameterRemote p = new ParameterRemote(j);
                        p.readMetaData(transfer);
                        parameters.add(p);
                    }
                }
            } catch (IOException e) {
                s.removeServer(e, i--, ++count);
            }
        }
    }

    public boolean isQuery() {
        return isQuery;
    }

    public ArrayList<ParameterInterface> getParameters() {
        return parameters;
    }

    private void prepareIfRequired() {
        if (session.getLastReconnect() != created) {
            // in this case we need to prepare again in every case
            id = Integer.MIN_VALUE;
        }
        session.checkClosed();
        if (id <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS) {
            // object is too old - we need to prepare again
            prepare(session, false);
        }
    }

    public ResultInterface getMetaData() {
        synchronized (session) {
            if (!isQuery) {
                return null;
            }
            int objectId = session.getNextId();
            ResultRemote result = null;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                Transfer transfer = transferList.get(i);
                try {
                    session.traceOperation("COMMAND_GET_META_DATA", id);
                    transfer.writeInt(SessionRemote.COMMAND_GET_META_DATA).writeInt(id).writeInt(objectId);
                    session.done(transfer);
                    int columnCount = transfer.readInt();
                    int rowCount = transfer.readInt();
                    result = new ResultRemoteInMemory(session, transfer, objectId, columnCount, rowCount, Integer.MAX_VALUE);
                    break;
                } catch (IOException e) {
                    session.removeServer(e, i--, ++count);
                }
            }
            session.autoCommitIfCluster();
            return result;
        }
    }

    public ResultInterface executeQuery(int maxRows, boolean scrollable) {
        checkParameters();
        synchronized (session) {
            int objectId = session.getNextId();
            ResultRemote result = null;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                Transfer transfer = transferList.get(i);
                try {
                    if (startTimestamp == null) {
                        session.traceOperation("COMMAND_EXECUTE_QUERY", id);
                        transfer.writeInt(SessionRemote.COMMAND_EXECUTE_QUERY).writeInt(id).writeInt(objectId).writeInt(maxRows);
                    } else {
                        transfer.writeInt(SessionRemote.COMMAND_EXECUTE_TRANSACTIONAL_QUERY).writeInt(id).writeInt(objectId)
                                .writeInt(maxRows).writeLong(startTimestamp.longValue());
                    }
                    int fetch;
                    if (session.isClustered() || scrollable) {
                        fetch = Integer.MAX_VALUE;
                    } else {
                        fetch = fetchSize;
                    }
                    transfer.writeInt(fetch);
                    sendParameters(transfer);
                    session.done(transfer);
                    int columnCount = transfer.readInt();
                    int rowCount = transfer.readInt();
                    if (result != null) {
                        result.close();
                        result = null;
                    }
                    if (rowCount < 0)
                        result = new ResultRemoteCursor(session, transfer, objectId, columnCount, fetch);
                    else
                        result = new ResultRemoteInMemory(session, transfer, objectId, columnCount, rowCount, fetch);
                    if (readonly) {
                        break;
                    }
                } catch (IOException e) {
                    session.removeServer(e, i--, ++count);
                }
            }
            session.autoCommitIfCluster();
            session.readSessionState();
            return result;
        }
    }

    public int executeUpdate() {
        checkParameters();
        synchronized (session) {
            int updateCount = 0;
            boolean autoCommit = false;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                Transfer transfer = transferList.get(i);
                try {
                    if (startTimestamp == null) {
                        session.traceOperation("COMMAND_EXECUTE_UPDATE", id);
                        transfer.writeInt(SessionRemote.COMMAND_EXECUTE_UPDATE).writeInt(id);
                        sendParameters(transfer);
                        session.done(transfer);
                        updateCount = transfer.readInt();
                        autoCommit = transfer.readBoolean();
                    } else {
                        session.traceOperation("COMMAND_EXECUTE_TRANSACTIONAL_UPDATE", id);
                        transfer.writeInt(SessionRemote.COMMAND_EXECUTE_TRANSACTIONAL_UPDATE).writeInt(id)
                                .writeLong(startTimestamp.longValue());
                        sendParameters(transfer);
                        session.done(transfer);
                        updateCount = transfer.readInt();
                        autoCommit = transfer.readBoolean();
                        int rowKeyCount = transfer.readInt();
                        transactionalRowKeys = new byte[rowKeyCount][];
                        for (int j = 0; j < rowKeyCount; j++)
                            transactionalRowKeys[j] = transfer.readBytes();
                    }
                } catch (IOException e) {
                    session.removeServer(e, i--, ++count);
                }
            }
            session.setAutoCommitFromServer(autoCommit);
            session.autoCommitIfCluster();
            session.readSessionState();
            return updateCount;
        }
    }

    private void checkParameters() {
        for (ParameterInterface p : parameters) {
            p.checkSet();
        }
    }

    private void sendParameters(Transfer transfer) throws IOException {
        int len = parameters.size();
        transfer.writeInt(len);
        for (ParameterInterface p : parameters) {
            transfer.writeValue(p.getParamValue());
        }
    }

    public void close() {
        if (session == null || session.isClosed()) {
            return;
        }
        synchronized (session) {
            session.traceOperation("COMMAND_CLOSE", id);
            for (Transfer transfer : transferList) {
                try {
                    transfer.writeInt(SessionRemote.COMMAND_CLOSE).writeInt(id);
                } catch (IOException e) {
                    trace.error(e, "close");
                }
            }
        }
        session = null;
        try {
            for (ParameterInterface p : parameters) {
                Value v = p.getParamValue();
                if (v != null) {
                    v.close();
                }
            }
        } catch (DbException e) {
            trace.error(e, "close");
        }
        parameters.clear();
    }

    /**
     * Cancel this current statement.
     */
    public void cancel() {
        session.cancelStatement(id);
    }

    public String toString() {
        return sql + Trace.formatParams(getParameters());
    }

    public int getCommandType() {
        return UNKNOWN;
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    @Override
    public byte[][] getTransactionalRowKeys() {
        return transactionalRowKeys;
    }

    @Override
    public Long getStartTimestamp() {
        return startTimestamp;
    }

    @Override
    public void setStartTimestamp(Long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }
}
