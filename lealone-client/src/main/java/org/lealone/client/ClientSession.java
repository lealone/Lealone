/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.exceptions.LealoneException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceSystem;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.SmallLRUCache;
import org.lealone.common.util.TempFileDeleter;
import org.lealone.db.Command;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.DataHandler;
import org.lealone.db.Session;
import org.lealone.db.SessionBase;
import org.lealone.db.SetTypes;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.value.Value;
import org.lealone.net.AsyncCallback;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetEndpoint;
import org.lealone.net.NetFactory;
import org.lealone.net.NetFactoryManager;
import org.lealone.net.TcpClientConnection;
import org.lealone.net.Transfer;
import org.lealone.sql.ParsedStatement;
import org.lealone.sql.PreparedStatement;
import org.lealone.storage.LobStorage;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;
import org.lealone.transaction.Transaction;

/**
 * The client side part of a session when using the server mode. 
 * This object communicates with a session on the server side.
 * 
 * @author H2 Group
 * @author zhh
 */
public class ClientSession extends SessionBase implements DataHandler, Transaction.Participant {

    private final ConnectionInfo ci;
    private final String server;
    private final Session parent;
    private final Object lobSyncObject = new Object();
    private LobStorage lobStorage;
    private TraceSystem traceSystem;
    private Trace trace;
    private Transfer transfer;
    private String cipher;
    private byte[] fileEncryptionKey;
    private int sessionId;
    private TcpClientConnection tcpConnection;

    ClientSession(ConnectionInfo ci, String server, Session parent) {
        if (!ci.isRemote()) {
            throw DbException.throwInternalError();
        }
        this.ci = ci;
        this.server = server;
        this.parent = parent;
    }

    TcpClientConnection getTcpConnection() {
        return tcpConnection;
    }

    @Override
    public int getSessionId() {
        return sessionId;
    }

    @Override
    public int getNextId() {
        if (tcpConnection == null)
            return super.getNextId();
        else
            return tcpConnection.getNextId();
    }

    /**
     * Open a new session.
     *
     * @return the session
     */
    @Override
    public Session connect() {
        initTraceSystem();
        cipher = ci.getProperty("CIPHER");
        if (cipher != null) {
            fileEncryptionKey = MathUtils.secureRandomBytes(32);
        }
        NetEndpoint endpoint = NetEndpoint.createTCP(server);
        try {
            transfer = initTransfer(ci, endpoint);
        } catch (Exception e) {
            closeTraceSystem();
            throw DbException.convert(e);
        }
        return this;
    }

    @Override
    public Session connect(boolean first) {
        return connect();
    }

    private void initTraceSystem() {
        traceSystem = new TraceSystem();
        String traceLevelFile = ci.getProperty(SetTypes.TRACE_LEVEL_FILE, null);
        if (traceLevelFile != null) {
            int level = Integer.parseInt(traceLevelFile);
            String prefix = getFilePrefix(SysProperties.CLIENT_TRACE_DIRECTORY, ci.getDatabaseName());
            try {
                traceSystem.setLevelFile(level);
                if (level > 0) {
                    String file = FileUtils.createTempFile(prefix, Constants.SUFFIX_TRACE_FILE, false, false);
                    traceSystem.setFileName(file);
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, prefix);
            }
        }
        String traceLevelSystemOut = ci.getProperty(SetTypes.TRACE_LEVEL_SYSTEM_OUT, null);
        if (traceLevelSystemOut != null) {
            int level = Integer.parseInt(traceLevelSystemOut);
            traceSystem.setLevelSystemOut(level);
        }
        trace = traceSystem.getTrace(Trace.JDBC);
    }

    private void closeTraceSystem() {
        traceSystem.close();
        traceSystem = null;
        trace = null;
    }

    private Transfer initTransfer(ConnectionInfo ci, NetEndpoint endpoint) throws Exception {
        NetFactory factory = NetFactoryManager.getFactory(ci.getNetFactoryName());
        CaseInsensitiveMap<String> config = new CaseInsensitiveMap<>(ci.getProperties());
        AsyncConnection conn = factory.getNetClient().createConnection(config, endpoint);
        if (!(conn instanceof TcpClientConnection)) {
            throw DbException.throwInternalError("not tcp client connection: " + conn.getClass().getName());
        }
        tcpConnection = (TcpClientConnection) conn;
        sessionId = getNextId();
        transfer = tcpConnection.createTransfer(this);
        tcpConnection.writeInitPacket(this, transfer, ci);
        if (isValid()) {
            tcpConnection.addSession(sessionId, this);
        }
        return transfer;
    }

    private static String getFilePrefix(String dir, String dbName) {
        StringBuilder buff = new StringBuilder(dir);
        if (!(dir.charAt(dir.length() - 1) == File.separatorChar))
            buff.append(File.separatorChar);
        for (int i = 0, length = dbName.length(); i < length; i++) {
            char ch = dbName.charAt(i);
            if (Character.isLetterOrDigit(ch)) {
                buff.append(ch);
            } else {
                buff.append('_');
            }
        }
        return buff.toString();
    }

    @Override
    public void cancel() {
        // this method is called when closing the connection
        // the statement that is currently running is not canceled in this case
        // however Statement.cancel is supported
    }

    /**
     * Cancel the statement with the given id.
     *
     * @param id the statement id
     */
    public void cancelStatement(int statementId) {
        try {
            transfer.writeRequestHeader(Session.SESSION_CANCEL_STATEMENT).writeInt(statementId).flush();
        } catch (IOException e) {
            trace.debug(e, "could not cancel statement");
        }
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        if (this.autoCommit == autoCommit)
            return;
        setAutoCommitSend(autoCommit);
        this.autoCommit = autoCommit;
    }

    private void setAutoCommitSend(boolean autoCommit) {
        try {
            int id = getNextId();
            traceOperation("SESSION_SET_AUTOCOMMIT", autoCommit ? 1 : 0);
            transfer.writeRequestHeader(id, Session.SESSION_SET_AUTO_COMMIT);
            transfer.writeBoolean(autoCommit);
            AsyncCallback<Void> ac = new AsyncCallback<>();
            transfer.addAsyncCallback(id, ac);
            transfer.flush();
            ac.await();
        } catch (IOException e) {
            handleException(e);
        }
    }

    public void handleException(Exception e) {
        checkClosed();
        if (e instanceof DbException)
            throw (DbException) e;
        throw new LealoneException(e);
    }

    @Override
    public Command createCommand(String sql, int fetchSize) {
        checkClosed();
        return new ClientCommand(this, transfer.copy(this), sql, fetchSize);
    }

    @Override
    public StorageCommand createStorageCommand() {
        checkClosed();
        return new ClientCommand(this, transfer.copy(this), null, -1);
    }

    @Override
    public Command prepareCommand(String sql, int fetchSize) {
        Command c = createCommand(sql, fetchSize);
        c.prepare();
        return c;
    }

    /**
     * Check if this session is closed and throws an exception if so.
     *
     * @throws DbException if the session is closed
     */
    public void checkClosed() {
        if (isClosed()) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "session closed");
        }
    }

    @Override
    public boolean isClosed() {
        return transfer == null;
    }

    @Override
    public void close() {
        RuntimeException closeError = null;
        synchronized (this) {
            try {
                traceOperation("SESSION_CLOSE", 0);
                // 只有当前Session有效时服务器端才持有对应的session
                if (isValid()) {
                    transfer.writeRequestHeader(Session.SESSION_CLOSE).flush();
                    tcpConnection.removeSession(sessionId);
                }
            } catch (RuntimeException e) {
                trace.error(e, "close");
                closeError = e;
            } catch (Exception e) {
                trace.error(e, "close");
            }

        }
        transfer = null;
        traceSystem.close();
        if (closeError != null) {
            throw closeError;
        }
    }

    @Override
    public Trace getTrace() {
        return traceSystem.getTrace(Trace.JDBC);
    }

    /**
     * Write the operation to the trace system if debug trace is enabled.
     *
     * @param operation the operation performed
     * @param id the id of the operation
     */
    public void traceOperation(String operation, int id) {
        if (trace.isDebugEnabled()) {
            trace.debug("{0} {1}", operation, id);
        }
    }

    @Override
    public void checkPowerOff() {
        // ok
    }

    @Override
    public void checkWritingAllowed() {
        // ok
    }

    @Override
    public String getDatabasePath() {
        return "";
    }

    @Override
    public String getLobCompressionAlgorithm(int type) {
        return null;
    }

    @Override
    public int getMaxLengthInplaceLob() {
        return SysProperties.LOB_CLIENT_MAX_SIZE_MEMORY;
    }

    @Override
    public FileStorage openFile(String name, String mode, boolean mustExist) {
        if (mustExist && !FileUtils.exists(name)) {
            throw DbException.get(ErrorCode.FILE_NOT_FOUND_1, name);
        }
        FileStorage fileStorage;
        if (cipher == null) {
            fileStorage = FileStorage.open(this, name, mode);
        } else {
            fileStorage = FileStorage.open(this, name, mode, cipher, fileEncryptionKey, 0);
        }
        fileStorage.setCheckedWriting(false);
        try {
            fileStorage.init();
        } catch (DbException e) {
            fileStorage.closeSilently();
            throw e;
        }
        return fileStorage;
    }

    @Override
    public DataHandler getDataHandler() {
        return this;
    }

    @Override
    public Object getLobSyncObject() {
        return lobSyncObject;
    }

    @Override
    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return null;
    }

    @Override
    public TempFileDeleter getTempFileDeleter() {
        return TempFileDeleter.getInstance();
    }

    @Override
    public LobStorage getLobStorage() {
        if (lobStorage == null) {
            lobStorage = new ClientLobStorage(this);
        }
        return lobStorage;
    }

    @Override
    public Connection getLobConnection() {
        return null;
    }

    @Override
    public synchronized int readLob(long lobId, byte[] hmac, long offset, byte[] buff, int off, int length) {
        try {
            int id = getNextId();
            traceOperation("LOB_READ", (int) lobId);
            transfer.writeRequestHeader(id, Session.COMMAND_READ_LOB);
            transfer.writeLong(lobId);
            transfer.writeBytes(hmac);
            transfer.writeLong(offset);
            transfer.writeInt(length);
            AtomicInteger lengthAI = new AtomicInteger();
            AsyncCallback<Void> ac = new AsyncCallback<Void>() {
                @Override
                public void runInternal() {
                    try {
                        int length = transfer.readInt();
                        if (length > 0) {
                            transfer.readBytes(buff, off, length);
                        }
                        lengthAI.set(length);
                    } catch (IOException e) {
                        throw DbException.convert(e);
                    }
                }
            };
            transfer.addAsyncCallback(id, ac);
            transfer.flush();
            ac.await();
            return lengthAI.get();
        } catch (IOException e) {
            handleException(e);
        }
        return 1;
    }

    @Override
    public synchronized void commitTransaction(String allLocalTransactionNames) {
        checkClosed();
        try {
            transfer.writeRequestHeader(Session.COMMAND_DISTRIBUTED_TRANSACTION_COMMIT);
            transfer.writeString(allLocalTransactionNames).flush();
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public synchronized void rollbackTransaction() {
        checkClosed();
        try {
            transfer.writeRequestHeader(Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK);
            transfer.flush();
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public synchronized void addSavepoint(String name) {
        checkClosed();
        try {
            transfer.writeRequestHeader(Session.COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT);
            transfer.writeString(name);
            transfer.flush();
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public synchronized void rollbackToSavepoint(String name) {
        checkClosed();
        try {
            transfer.writeRequestHeader(Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK_SAVEPOINT);
            transfer.writeString(name);
            transfer.flush();
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public synchronized boolean validateTransaction(String localTransactionName) {
        checkClosed();
        try {
            int id = getNextId();
            transfer.writeRequestHeader(id, Session.COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE);
            transfer.writeString(localTransactionName);
            AtomicBoolean isValid = new AtomicBoolean();
            AsyncCallback<Void> ac = new AsyncCallback<Void>() {
                @Override
                public void runInternal() {
                    try {
                        isValid.set(transfer.readBoolean());
                    } catch (IOException e) {
                        throw DbException.convert(e);
                    }
                }
            };
            transfer.addAsyncCallback(id, ac);
            transfer.flush();
            ac.await();
            return isValid.get();
        } catch (Exception e) {
            handleException(e);
            return false;
        }
    }

    public synchronized ClientBatchCommand getClientBatchCommand(ArrayList<String> batchCommands) {
        checkClosed();
        return new ClientBatchCommand(this, transfer.copy(this), batchCommands);
    }

    public synchronized ClientBatchCommand getClientBatchCommand(Command preparedCommand,
            ArrayList<Value[]> batchParameters) {
        checkClosed();
        return new ClientBatchCommand(this, transfer.copy(this), preparedCommand, batchParameters);
    }

    @Override
    public String getURL() {
        return ci.getURL();
    }

    @Override
    public ParsedStatement parseStatement(String sql) {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int fetchSize) {
        return null;
    }

    @Override
    public int getModificationId() {
        return 0;
    }

    @Override
    public void rollback() {
    }

    @Override
    public void setRoot(boolean isRoot) {
    }

    @Override
    public void commit(String allLocalTransactionNames) {
    }

    @Override
    public ConnectionInfo getConnectionInfo() {
        return ci;
    }

    @Override
    public void runModeChanged(String newTargetEndpoints) {
        parent.runModeChanged(newTargetEndpoints);
    }
}
