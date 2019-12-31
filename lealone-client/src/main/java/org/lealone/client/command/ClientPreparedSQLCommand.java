/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.lealone.client.result.ClientResult;
import org.lealone.client.result.RowCountDeterminedClientResult;
import org.lealone.client.session.ClientSession;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.util.Utils;
import org.lealone.db.CommandParameter;
import org.lealone.db.SysProperties;
import org.lealone.db.async.Future;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.net.TransferInputStream;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.batch.BatchStatementPreparedUpdate;
import org.lealone.server.protocol.batch.BatchStatementUpdateAck;
import org.lealone.server.protocol.dt.DistributedTransactionPreparedQuery;
import org.lealone.server.protocol.dt.DistributedTransactionPreparedUpdate;
import org.lealone.server.protocol.dt.DistributedTransactionQueryAck;
import org.lealone.server.protocol.dt.DistributedTransactionUpdateAck;
import org.lealone.server.protocol.ps.PreparedStatementClose;
import org.lealone.server.protocol.ps.PreparedStatementGetMetaData;
import org.lealone.server.protocol.ps.PreparedStatementGetMetaDataAck;
import org.lealone.server.protocol.ps.PreparedStatementPrepare;
import org.lealone.server.protocol.ps.PreparedStatementPrepareAck;
import org.lealone.server.protocol.ps.PreparedStatementPrepareReadParams;
import org.lealone.server.protocol.ps.PreparedStatementPrepareReadParamsAck;
import org.lealone.server.protocol.ps.PreparedStatementQuery;
import org.lealone.server.protocol.ps.PreparedStatementUpdate;
import org.lealone.server.protocol.replication.ReplicationPreparedUpdate;
import org.lealone.server.protocol.statement.StatementQueryAck;
import org.lealone.server.protocol.statement.StatementUpdateAck;
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
        // commandId重新prepare时会变，但是parameters不会变
        parameters = Utils.newSmallArrayList();
        prepare(true);
    }

    @Override
    public int getType() {
        return CLIENT_PREPARED_SQL_COMMAND;
    }

    private void prepare(boolean readParams) {
        // Prepared SQL的ID，每次执行时都发给后端
        commandId = session.getNextId();
        if (readParams) {
            PreparedStatementPrepareReadParams packet = new PreparedStatementPrepareReadParams(commandId, sql);
            Future<PreparedStatementPrepareReadParamsAck> f = session.send(packet);
            PreparedStatementPrepareReadParamsAck ack = f.get();
            isQuery = ack.isQuery;
            parameters = new ArrayList<>(ack.params);
        } else {
            PreparedStatementPrepare packet = new PreparedStatementPrepare(commandId, sql);
            Future<PreparedStatementPrepareAck> f = session.send(packet);
            PreparedStatementPrepareAck ack = f.get();
            isQuery = ack.isQuery;
        }
    }

    private void prepareIfRequired() {
        session.checkClosed();
        if (commandId <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS) {
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
        prepareIfRequired();
        try {
            Future<PreparedStatementGetMetaDataAck> f = session.send(new PreparedStatementGetMetaData(commandId));
            PreparedStatementGetMetaDataAck ack = f.get();
            ClientResult result = new RowCountDeterminedClientResult(session, (TransferInputStream) ack.in, -1,
                    ack.columnCount, 0, 0);
            return result;
        } catch (IOException e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    protected Future<Result> query(int maxRows, boolean scrollable, List<PageKey> pageKeys) {
        isQuery = true;
        checkParameters();
        prepareIfRequired();
        int fetch;
        if (scrollable) {
            fetch = Integer.MAX_VALUE;
        } else {
            fetch = fetchSize;
        }
        try {
            int resultId = session.getNextId();

            int size = parameters.size();
            Value[] values = new Value[size];
            for (int i = 0; i < size; i++) {
                values[i] = parameters.get(i).getValue();
            }
            Packet packet;
            if (isDistributed()) {
                packet = new DistributedTransactionPreparedQuery(pageKeys, resultId, maxRows, fetch, scrollable,
                        commandId, size, values);
                return session.<Result, DistributedTransactionQueryAck> send(packet, ack -> {
                    session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                    return getQueryResult(ack, fetch, resultId);
                });
            } else {
                packet = new PreparedStatementQuery(pageKeys, resultId, maxRows, fetch, scrollable, commandId, size,
                        values);
                return session.<Result, StatementQueryAck> send(packet, ack -> {
                    return getQueryResult(ack, fetch, resultId);
                });
            }
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    @Override
    protected Future<Integer> update(String replicationName, List<PageKey> pageKeys) {
        checkParameters();
        prepareIfRequired();
        int size = parameters.size();
        Value[] values = new Value[size];
        for (int i = 0; i < size; i++) {
            values[i] = parameters.get(i).getValue();
        }
        Packet packet;
        if (isDistributed()) {
            packet = new DistributedTransactionPreparedUpdate(pageKeys, commandId, size, values);
            return session.<Integer, DistributedTransactionUpdateAck> send(packet, ack -> {
                session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                return ack.updateCount;
            });
        } else {
            if (replicationName != null)
                packet = new ReplicationPreparedUpdate(pageKeys, commandId, size, values, replicationName);
            else
                packet = new PreparedStatementUpdate(pageKeys, commandId, size, values);
            return session.<Integer, StatementUpdateAck> send(packet, ack -> {
                return ack.updateCount;
            });
        }
    }

    private void checkParameters() {
        for (CommandParameter p : parameters) {
            p.checkSet();
        }
    }

    @Override
    public void close() {
        if (session == null || session.isClosed()) {
            return;
        }
        int packetId = session.getNextId();
        session.traceOperation("COMMAND_CLOSE", packetId);
        try {
            session.send(new PreparedStatementClose(commandId));
        } catch (Exception e) {
            session.getTrace().error(e, "close session");
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
                session.getTrace().error(e, "close command parameters");
            }
            parameters = null;
        }
        session = null;
    }

    @Override
    public String toString() {
        return sql + Trace.formatParams(getParameters());
    }

    public int[] executeBatchPreparedSQLCommands(List<Value[]> batchParameters) {
        try {

            Future<BatchStatementUpdateAck> f = session
                    .send(new BatchStatementPreparedUpdate(commandId, batchParameters.size(), batchParameters));
            BatchStatementUpdateAck ack = f.get();
            return ack.results;
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }
}
