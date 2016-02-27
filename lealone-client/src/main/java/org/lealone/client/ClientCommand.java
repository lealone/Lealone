/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;

import org.lealone.api.ErrorCode;
import org.lealone.client.result.ClientResult;
import org.lealone.client.result.RowCountDeterminedClientResult;
import org.lealone.client.result.RowCountUndeterminedClientResult;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.util.New;
import org.lealone.db.CommandParameter;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.db.result.Result;
import org.lealone.db.value.Transfer;
import org.lealone.db.value.Value;
import org.lealone.storage.StorageCommand;

/**
 * Represents the client-side part of a SQL statement.
 * This class is not used in embedded mode.
 * 
 * @author H2 Group
 * @author zhh
 */
public class ClientCommand implements StorageCommand {

    private final Transfer transfer;
    private final ArrayList<CommandParameter> parameters;
    private final Trace trace;
    private final String sql;
    private final int fetchSize;
    private final boolean prepared;
    private ClientSession session;
    private int id;
    private boolean isQuery;

    public ClientCommand(ClientSession session, Transfer transfer, String sql, int fetchSize, boolean prepare) {
        this.transfer = transfer;
        parameters = New.arrayList();
        trace = session.getTrace();
        this.sql = sql;
        this.fetchSize = fetchSize;
        if (prepare) {
            prepared = true;
            prepare(session, true);
        } else {
            prepared = false;
            id = session.getNextId();
        }
        // set session late because prepare might fail - in this case we don't
        // need to close the object
        this.session = session;
    }

    private void prepare(ClientSession s, boolean createParams) {
        id = s.getNextId();
        try {
            if (createParams) {
                s.traceOperation("COMMAND_PREPARE_READ_PARAMS", id);
                transfer.writeInt(Session.COMMAND_PREPARE_READ_PARAMS);
            } else {
                s.traceOperation("COMMAND_PREPARE", id);
                transfer.writeInt(Session.COMMAND_PREPARE);
            }
            transfer.writeInt(id).writeString(sql);
            s.done(transfer);
            isQuery = transfer.readBoolean();
            if (createParams) {
                parameters.clear();
                int paramCount = transfer.readInt();
                for (int j = 0; j < paramCount; j++) {
                    ClientCommandParameter p = new ClientCommandParameter(j);
                    p.readMetaData(transfer);
                    parameters.add(p);
                }
            }
        } catch (IOException e) {
            s.handleException(e);
        }
    }

    @Override
    public boolean isQuery() {
        return isQuery;
    }

    @Override
    public ArrayList<CommandParameter> getParameters() {
        return parameters;
    }

    private void prepareIfRequired() {
        session.checkClosed();
        if (id <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS) {
            // object is too old - we need to prepare again
            prepare(session, false);
        }
    }

    @Override
    public Result getMetaData() {
        synchronized (session) {
            if (!isQuery) {
                return null;
            }
            int objectId = session.getNextId();
            ClientResult result = null;
            prepareIfRequired();
            try {
                session.traceOperation("COMMAND_GET_META_DATA", id);
                transfer.writeInt(Session.COMMAND_GET_META_DATA).writeInt(id).writeInt(objectId);
                session.done(transfer);
                int columnCount = transfer.readInt();
                int rowCount = transfer.readInt();
                result = new RowCountDeterminedClientResult(session, transfer, objectId, columnCount, rowCount,
                        Integer.MAX_VALUE);
            } catch (IOException e) {
                session.handleException(e);
            }
            return result;
        }
    }

    @Override
    public Result query(int maxRows) {
        return query(maxRows, false);
    }

    @Override
    public Result query(int maxRows, boolean scrollable) {
        synchronized (session) {
            if (prepared)
                return executePreparedQuery(maxRows, scrollable);
            else
                return executeQueryDirectly(maxRows, scrollable);
        }
    }

    private Result executeQueryDirectly(int maxRows, boolean scrollable) {
        int objectId = session.getNextId();
        ClientResult result = null;
        try {
            boolean isDistributedQuery = session.getTransaction() != null && !session.getTransaction().isAutoCommit();
            if (isDistributedQuery) {
                session.traceOperation("COMMAND_DISTRIBUTED_TRANSACTION_QUERY", id);
                transfer.writeInt(Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY);
            } else {
                session.traceOperation("COMMAND_QUERY", id);
                transfer.writeInt(Session.COMMAND_QUERY);
            }
            transfer.writeInt(id).writeString(sql).writeInt(objectId).writeInt(maxRows);
            int fetch;
            if (scrollable) {
                fetch = Integer.MAX_VALUE;
            } else {
                fetch = fetchSize;
            }
            transfer.writeInt(fetch);
            session.done(transfer);

            if (isDistributedQuery)
                session.getTransaction().addLocalTransactionNames(transfer.readString());

            int columnCount = transfer.readInt();
            int rowCount = transfer.readInt();

            if (rowCount < 0)
                result = new RowCountUndeterminedClientResult(session, transfer, objectId, columnCount, fetch);
            else
                result = new RowCountDeterminedClientResult(session, transfer, objectId, columnCount, rowCount, fetch);
        } catch (Exception e) {
            session.handleException(e);
        }
        session.readSessionState();
        isQuery = true;
        return result;
    }

    private Result executePreparedQuery(int maxRows, boolean scrollable) {
        checkParameters();
        int objectId = session.getNextId();
        ClientResult result = null;
        prepareIfRequired();
        try {
            boolean isDistributedQuery = session.getTransaction() != null && !session.getTransaction().isAutoCommit();
            if (isDistributedQuery) {
                session.traceOperation("COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY", id);
                transfer.writeInt(Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY);
            } else {
                session.traceOperation("COMMAND_PREPARED_QUERY", id);
                transfer.writeInt(Session.COMMAND_PREPARED_QUERY);
            }
            transfer.writeInt(id).writeInt(objectId).writeInt(maxRows);
            int fetch;
            if (scrollable) {
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

            if (rowCount < 0)
                result = new RowCountUndeterminedClientResult(session, transfer, objectId, columnCount, fetch);
            else
                result = new RowCountDeterminedClientResult(session, transfer, objectId, columnCount, rowCount, fetch);
        } catch (Exception e) {
            session.handleException(e);
        }
        session.readSessionState();
        return result;
    }

    @Override
    public int update() {
        synchronized (session) {
            if (prepared)
                return executePreparedUpdate(null);
            else
                return update(null);
        }
    }

    @Override
    public int update(String replicationName) {
        synchronized (session) {
            if (prepared)
                return executePreparedUpdate(replicationName);
            else
                return executeUpdateDirectly(replicationName);
        }
    }

    private int executeUpdateDirectly(String replicationName) {
        int updateCount = 0;
        try {
            boolean isDistributedUpdate = session.getTransaction() != null && !session.getTransaction().isAutoCommit();
            if (isDistributedUpdate) {
                session.traceOperation("COMMAND_DISTRIBUTED_TRANSACTION_UPDATE", id);
                transfer.writeInt(Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE);
            } else if (replicationName != null) {
                session.traceOperation("COMMAND_REPLICATION_UPDATE", id);
                transfer.writeInt(Session.COMMAND_REPLICATION_UPDATE);
            } else {
                session.traceOperation("COMMAND_UPDATE", id);
                transfer.writeInt(Session.COMMAND_UPDATE);
            }
            transfer.writeInt(id).writeString(sql);
            if (replicationName != null)
                transfer.writeString(replicationName);
            session.done(transfer);

            if (isDistributedUpdate)
                session.getTransaction().addLocalTransactionNames(transfer.readString());

            updateCount = transfer.readInt();
        } catch (Exception e) {
            session.handleException(e);
        }
        session.readSessionState();
        return updateCount;
    }

    private int executePreparedUpdate(String replicationName) {
        checkParameters();
        int updateCount = 0;
        prepareIfRequired();
        try {
            boolean isDistributedUpdate = session.getTransaction() != null && !session.getTransaction().isAutoCommit();
            if (isDistributedUpdate) {
                session.traceOperation("COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE", id);
                transfer.writeInt(Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE);
            } else if (replicationName != null) {
                session.traceOperation("COMMAND_REPLICATION_PREPARED_UPDATE", id);
                transfer.writeInt(Session.COMMAND_REPLICATION_PREPARED_UPDATE);
            } else {
                session.traceOperation("COMMAND_PREPARED_UPDATE", id);
                transfer.writeInt(Session.COMMAND_PREPARED_UPDATE);
            }
            transfer.writeInt(id);
            if (replicationName != null)
                transfer.writeString(replicationName);
            sendParameters(transfer);
            session.done(transfer);

            if (isDistributedUpdate)
                session.getTransaction().addLocalTransactionNames(transfer.readString());

            updateCount = transfer.readInt();

        } catch (Exception e) {
            session.handleException(e);
        }
        session.readSessionState();
        return updateCount;
    }

    private void checkParameters() {
        for (CommandParameter p : parameters) {
            p.checkSet();
        }
    }

    private void sendParameters(Transfer transfer) throws IOException {
        int len = parameters.size();
        transfer.writeInt(len);
        for (CommandParameter p : parameters) {
            transfer.writeValue(p.getValue());
        }
    }

    @Override
    public void close() {
        if (session == null || session.isClosed()) {
            return;
        }
        synchronized (session) {
            session.traceOperation("COMMAND_CLOSE", id);
            try {
                transfer.writeInt(Session.COMMAND_CLOSE).writeInt(id);
            } catch (IOException e) {
                trace.error(e, "close");
            }
        }
        session = null;
        try {
            for (CommandParameter p : parameters) {
                Value v = p.getValue();
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
    public int getType() {
        return CLIENT_COMMAND;
    }

    int getId() {
        return id;
    }

    String getSql() {
        return sql;
    }

    @Override
    public Object executePut(String replicationName, String mapName, ByteBuffer key, ByteBuffer value) {
        byte[] bytes = null;
        try {
            boolean isDistributedUpdate = session.getTransaction() != null && !session.getTransaction().isAutoCommit();
            if (isDistributedUpdate) {
                session.traceOperation("COMMAND_STORAGE_DISTRIBUTED_PUT", id);
                transfer.writeInt(Session.COMMAND_STORAGE_DISTRIBUTED_PUT);
            } else if (replicationName != null) {
                session.traceOperation("COMMAND_STORAGE_REPLICATION_PUT", id);
                transfer.writeInt(Session.COMMAND_STORAGE_REPLICATION_PUT);
            } else {
                session.traceOperation("COMMAND_STORAGE_PUT", id);
                transfer.writeInt(Session.COMMAND_STORAGE_PUT);
            }
            transfer.writeString(mapName).writeByteBuffer(key).writeByteBuffer(value);
            if (replicationName != null)
                transfer.writeString(replicationName);
            session.done(transfer);

            if (isDistributedUpdate)
                session.getTransaction().addLocalTransactionNames(transfer.readString());

            bytes = transfer.readBytes();
        } catch (Exception e) {
            session.handleException(e);
        }
        session.readSessionState();
        return bytes;
    }

    @Override
    public Object executeGet(String mapName, ByteBuffer key) {
        byte[] bytes = null;
        try {
            boolean isDistributedUpdate = session.getTransaction() != null && !session.getTransaction().isAutoCommit();
            if (isDistributedUpdate) {
                session.traceOperation("COMMAND_STORAGE_DISTRIBUTED_GET", id);
                transfer.writeInt(Session.COMMAND_STORAGE_DISTRIBUTED_GET);
            } else {
                session.traceOperation("COMMAND_STORAGE_GET", id);
                transfer.writeInt(Session.COMMAND_STORAGE_GET);
            }
            transfer.writeString(mapName).writeByteBuffer(key);
            session.done(transfer);

            if (isDistributedUpdate)
                session.getTransaction().addLocalTransactionNames(transfer.readString());

            bytes = transfer.readBytes();
        } catch (Exception e) {
            session.handleException(e);
        }
        session.readSessionState();
        return bytes;
    }

    @Override
    public void moveLeafPage(String mapName, ByteBuffer splitKey, ByteBuffer page) {
        try {
            session.traceOperation("COMMAND_STORAGE_MOVE_LEAF_PAGE", id);
            transfer.writeInt(Session.COMMAND_STORAGE_MOVE_LEAF_PAGE);
            transfer.writeString(mapName).writeByteBuffer(splitKey).writeByteBuffer(page);
            session.done(transfer);
        } catch (Exception e) {
            session.handleException(e);
        }
        session.readSessionState();
    }

    @Override
    public void removeLeafPage(String mapName, ByteBuffer key) {
        try {
            session.traceOperation("COMMAND_STORAGE_REMOVE_LEAF_PAGE", id);
            transfer.writeInt(Session.COMMAND_STORAGE_REMOVE_LEAF_PAGE);
            transfer.writeString(mapName).writeByteBuffer(key);
            session.done(transfer);
        } catch (Exception e) {
            session.handleException(e);
        }
        session.readSessionState();
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
         * Read the parameter meta data from the transfer object.
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
