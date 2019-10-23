/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import org.lealone.client.result.ClientResult;
import org.lealone.client.result.RowCountDeterminedClientResult;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.util.Utils;
import org.lealone.db.CommandParameter;
import org.lealone.db.CommandUpdateResult;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.net.AsyncCallback;
import org.lealone.net.TransferInputStream;
import org.lealone.net.TransferOutputStream;
import org.lealone.storage.PageKey;

/**
 * Represents the client-side part of a prepared SQL statement.
 * This class is not used in embedded mode.
 * 
 * @author H2 Group
 * @author zhh
 */
public class ClientPreparedSQLCommand extends ClientSQLCommand {

    private ArrayList<CommandParameter> parameters;

    public ClientPreparedSQLCommand(ClientSession session, String sql, int fetchSize) {
        super(session, sql, fetchSize);
        packetId = session.getNextId();
        parameters = Utils.newSmallArrayList();
        prepare(true);
    }

    @Override
    public int getType() {
        return CLIENT_PREPARED_SQL_COMMAND;
    }

    private void prepare(final boolean readParams) {
        try {
            TransferOutputStream out = session.newOut();
            if (readParams) {
                session.traceOperation("COMMAND_PREPARE_READ_PARAMS", packetId);
                out.writeRequestHeader(packetId, Session.COMMAND_PREPARE_READ_PARAMS);
            } else {
                session.traceOperation("COMMAND_PREPARE", packetId);
                out.writeRequestHeader(packetId, Session.COMMAND_PREPARE);
            }
            out.writeString(sql);
            out.flushAndAwait(packetId, new AsyncCallback<Void>() {
                @Override
                public void runInternal(TransferInputStream in) throws Exception {
                    isQuery = in.readBoolean();
                    if (readParams) {
                        parameters.clear();
                        int paramCount = in.readInt();
                        for (int i = 0; i < paramCount; i++) {
                            ClientCommandParameter p = new ClientCommandParameter(i);
                            p.readMetaData(in);
                            parameters.add(p);
                        }
                    }
                }
            });
        } catch (IOException e) {
            session.handleException(e);
        }
    }

    private void prepareIfRequired() {
        session.checkClosed();
        if (packetId <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS) {
            // object is too old - we need to prepare again
            prepare(false);
        }
    }

    @Override
    public ArrayList<CommandParameter> getParameters() {
        return parameters;
    }

    @Override
    public Result getMetaData() {
        if (!isQuery) {
            return null;
        }
        TransferOutputStream out = session.newOut();
        int objectId = session.getNextId();
        ClientResult result = null;
        prepareIfRequired();
        try {
            session.traceOperation("COMMAND_GET_META_DATA", packetId);
            out.writeRequestHeader(packetId, Session.COMMAND_GET_META_DATA);
            out.writeInt(objectId);
            AsyncCallback<ClientResult> ac = new AsyncCallback<ClientResult>() {
                @Override
                public void runInternal(TransferInputStream in) throws Exception {
                    int columnCount = in.readInt();
                    int rowCount = in.readInt();
                    ClientResult result = new RowCountDeterminedClientResult(session, in, objectId, columnCount,
                            rowCount, Integer.MAX_VALUE);

                    setResult(result);
                }
            };
            result = out.flushAndAwait(packetId, ac);
        } catch (IOException e) {
            session.handleException(e);
        }
        return result;
    }

    @Override
    protected Result query(int maxRows, boolean scrollable, List<PageKey> pageKeys,
            AsyncHandler<AsyncResult<Result>> handler) {
        checkParameters();
        prepareIfRequired();
        String operation;
        int packetType;
        boolean isDistributedQuery = isDistributed();
        if (isDistributedQuery) {
            operation = "COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY";
            packetType = Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY;
        } else {
            operation = "COMMAND_PREPARED_QUERY";
            packetType = Session.COMMAND_PREPARED_QUERY;
        }
        int fetch;
        if (scrollable) {
            fetch = Integer.MAX_VALUE;
        } else {
            fetch = fetchSize;
        }
        try {
            TransferOutputStream out = session.newOut();
            int resultId = session.getNextId();
            writeQueryHeader(out, operation, packetType, resultId, maxRows, fetch, scrollable, pageKeys);
            writeParameters(out);
            return getQueryResult(out, isDistributedQuery, fetch, resultId, handler);
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    protected int update(String replicationName, CommandUpdateResult commandUpdateResult, List<PageKey> pageKeys,
            AsyncHandler<AsyncResult<Integer>> handler) {
        checkParameters();
        prepareIfRequired();
        String operation;
        int packetType;
        boolean isDistributedUpdate = isDistributed();
        if (isDistributedUpdate) {
            operation = "COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE";
            packetType = Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE;
        } else if (replicationName != null) {
            operation = "COMMAND_REPLICATION_PREPARED_UPDATE";
            packetType = Session.COMMAND_REPLICATION_PREPARED_UPDATE;
        } else {
            operation = "COMMAND_PREPARED_UPDATE";
            packetType = Session.COMMAND_PREPARED_UPDATE;
        }
        try {
            TransferOutputStream out = session.newOut();
            writeUpdateHeader(out, operation, packetType, replicationName, pageKeys);
            writeParameters(out);
            return getUpdateCount(out, isDistributedUpdate, commandUpdateResult, handler);
        } catch (Exception e) {
            session.handleException(e);
        }
        return 0;
    }

    private void checkParameters() {
        for (CommandParameter p : parameters) {
            p.checkSet();
        }
    }

    private void writeParameters(TransferOutputStream out) throws IOException {
        int len = parameters.size();
        out.writeInt(len);
        for (CommandParameter p : parameters) {
            out.writeValue(p.getValue());
        }
    }

    @Override
    public void close() {
        if (session == null || session.isClosed()) {
            return;
        }
        session.traceOperation("COMMAND_CLOSE", packetId);
        try {
            session.newOut().writeRequestHeader(packetId, Session.COMMAND_CLOSE).flush();
        } catch (IOException e) {
            session.getTrace().error(e, "close");
        }
        if (parameters != null) {
            try {
                for (CommandParameter p : parameters) {
                    Value v = p.getValue();
                    if (v != null) {
                        v.close();
                    }
                }
            } catch (DbException e) {
                session.getTrace().error(e, "close");
            }
            parameters = null;
        }
        session = null;
    }

    @Override
    public String toString() {
        return sql + Trace.formatParams(getParameters());
    }

    /**
     * A client side parameter.
     */
    private static class ClientCommandParameter implements CommandParameter {

        private final int index;
        private Value value;
        private int dataType = Value.UNKNOWN;
        private long precision;
        private int scale;
        private int nullable = ResultSetMetaData.columnNullableUnknown;

        public ClientCommandParameter(int index) {
            this.index = index;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public void setValue(Value newValue, boolean closeOld) {
            if (closeOld && value != null) {
                value.close();
            }
            value = newValue;
        }

        @Override
        public void setValue(Value value) {
            this.value = value;
        }

        @Override
        public Value getValue() {
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
         * Read the parameter meta data from the out object.
         *
         * @param in the TransferInputStream
         */
        public void readMetaData(TransferInputStream in) throws IOException {
            dataType = in.readInt();
            precision = in.readLong();
            scale = in.readInt();
            nullable = in.readInt();
        }
    }
}
