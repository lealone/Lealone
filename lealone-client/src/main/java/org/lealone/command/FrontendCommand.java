/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;

import org.lealone.api.ErrorCode;
import org.lealone.api.ParameterInterface;
import org.lealone.engine.FrontendSession;
import org.lealone.engine.SysProperties;
import org.lealone.message.DbException;
import org.lealone.message.Trace;
import org.lealone.result.ResultInterface;
import org.lealone.result.ResultRemote;
import org.lealone.result.ResultRemoteCursor;
import org.lealone.result.ResultRemoteInMemory;
import org.lealone.util.New;
import org.lealone.value.Transfer;
import org.lealone.value.Value;

/**
 * Represents the client-side part of a SQL statement.
 * This class is not used in embedded mode.
 */
public class FrontendCommand implements CommandInterface {

    private final ArrayList<Transfer> transferList;
    private final ArrayList<ParameterInterface> parameters;
    private final Trace trace;
    private final String sql;
    private final int fetchSize;
    private FrontendSession session;
    private int id;
    private boolean isQuery;
    private boolean readonly;
    private final int created;

    public FrontendCommand(FrontendSession session, ArrayList<Transfer> transferList, String sql, int fetchSize) {
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

    private void prepare(FrontendSession s, boolean createParams) {
        id = s.getNextId();
        for (int i = 0, count = 0; i < transferList.size(); i++) {
            try {
                Transfer transfer = transferList.get(i);
                if (createParams) {
                    s.traceOperation("SESSION_PREPARE_READ_PARAMS", id);
                    transfer.writeInt(FrontendSession.SESSION_PREPARE_READ_PARAMS).writeInt(id).writeString(sql);
                } else {
                    s.traceOperation("SESSION_PREPARE", id);
                    transfer.writeInt(FrontendSession.SESSION_PREPARE).writeInt(id).writeString(sql);
                }
                s.done(transfer);
                isQuery = transfer.readBoolean();
                readonly = transfer.readBoolean();
                int paramCount = transfer.readInt();
                if (createParams) {
                    parameters.clear();
                    for (int j = 0; j < paramCount; j++) {
                        Parameter p = new Parameter(j);
                        p.readMetaData(transfer);
                        parameters.add(p);
                    }
                }
            } catch (IOException e) {
                s.removeServer(e, i--, ++count);
            }
        }
    }

    @Override
    public boolean isQuery() {
        return isQuery;
    }

    @Override
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

    @Override
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
                    transfer.writeInt(FrontendSession.COMMAND_GET_META_DATA).writeInt(id).writeInt(objectId);
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

    @Override
    public ResultInterface executeQuery(int maxRows, boolean scrollable) {
        checkParameters();
        synchronized (session) {
            int objectId = session.getNextId();
            ResultRemote result = null;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                Transfer transfer = transferList.get(i);
                try {
                    boolean isDistributedQuery = session.getTransaction() != null && !session.getTransaction().isAutoCommit();
                    if (isDistributedQuery) {
                        session.traceOperation("COMMAND_EXECUTE_DISTRIBUTED_QUERY", id);
                        transfer.writeInt(FrontendSession.COMMAND_EXECUTE_DISTRIBUTED_QUERY).writeInt(id).writeInt(objectId)
                                .writeInt(maxRows);
                    } else {
                        session.traceOperation("COMMAND_EXECUTE_QUERY", id);
                        transfer.writeInt(FrontendSession.COMMAND_EXECUTE_QUERY) //
                                .writeInt(id).writeInt(objectId).writeInt(maxRows);
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

                    if (isDistributedQuery)
                        session.getTransaction().addLocalTransactionNames(transfer.readString());

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

    @Override
    public int executeUpdate() {
        checkParameters();
        synchronized (session) {
            int updateCount = 0;
            boolean autoCommit = false;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                Transfer transfer = transferList.get(i);
                try {
                    boolean isDistributedUpdate = session.getTransaction() != null && !session.getTransaction().isAutoCommit();
                    if (isDistributedUpdate) {
                        session.traceOperation("COMMAND_EXECUTE_DISTRIBUTED_UPDATE", id);
                        transfer.writeInt(FrontendSession.COMMAND_EXECUTE_DISTRIBUTED_UPDATE).writeInt(id);
                    } else {
                        session.traceOperation("COMMAND_EXECUTE_UPDATE", id);
                        transfer.writeInt(FrontendSession.COMMAND_EXECUTE_UPDATE).writeInt(id);
                    }
                    sendParameters(transfer);
                    session.done(transfer);

                    if (isDistributedUpdate)
                        session.getTransaction().addLocalTransactionNames(transfer.readString());

                    updateCount = transfer.readInt();
                    autoCommit = transfer.readBoolean();
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

    @Override
    public void close() {
        if (session == null || session.isClosed()) {
            return;
        }
        synchronized (session) {
            session.traceOperation("COMMAND_CLOSE", id);
            for (Transfer transfer : transferList) {
                try {
                    transfer.writeInt(FrontendSession.COMMAND_CLOSE).writeInt(id);
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
    @Override
    public void cancel() {
        session.cancelStatement(id);
    }

    @Override
    public String toString() {
        return sql + Trace.formatParams(getParameters());
    }

    @Override
    public int getCommandType() {
        return UNKNOWN;
    }

    int getId() {
        return id;
    }

    /**
     * A client side (remote) parameter.
     */
    private static class Parameter implements ParameterInterface {

        private Value value;
        private final int index;
        private int dataType = Value.UNKNOWN;
        private long precision;
        private int scale;
        private int nullable = ResultSetMetaData.columnNullableUnknown;

        public Parameter(int index) {
            this.index = index;
        }

        @Override
        public void setValue(Value newValue, boolean closeOld) {
            if (closeOld && value != null) {
                value.close();
            }
            value = newValue;
        }

        @Override
        public Value getParamValue() {
            return value;
        }

        @Override
        public void checkSet() {
            if (value == null) {
                throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, "#" + (index + 1));
            }
        }

        @Override
        public boolean isValueSet() {
            return value != null;
        }

        @Override
        public int getType() {
            return value == null ? dataType : value.getType();
        }

        @Override
        public long getPrecision() {
            return value == null ? precision : value.getPrecision();
        }

        @Override
        public int getScale() {
            return value == null ? scale : value.getScale();
        }

        @Override
        public int getNullable() {
            return nullable;
        }

        /**
         * Write the parameter meta data from the transfer object.
         *
         * @param transfer the transfer object
         */
        public void readMetaData(Transfer transfer) throws IOException {
            dataType = transfer.readInt();
            precision = transfer.readLong();
            scale = transfer.readInt();
            nullable = transfer.readInt();
        }

    }
}
