/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.sql.SQLException;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.exceptions.JdbcSQLException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.session.Session;

public abstract class TransferConnection extends AsyncConnection {

    private static final Logger logger = LoggerFactory.getLogger(TransferConnection.class);

    protected final ByteBuffer packetLengthByteBuffer = ByteBuffer
            .allocate(getPacketLengthByteBufferCapacity());

    protected final TransferInputStream in;
    protected final TransferOutputStream out;

    public TransferConnection(WritableChannel writableChannel, boolean isServer) {
        this(writableChannel, isServer, null, null);
    }

    public TransferConnection(WritableChannel writableChannel, boolean isServer, NetBuffer inBuffer,
            NetBuffer outBuffer) {
        super(writableChannel, isServer);
        if (inBuffer != null) {
            in = new TransferInputStream(inBuffer);
            out = createTransferOutputStream(outBuffer);
        } else {
            in = null;
            out = null;
        }
    }

    public int getPacketLengthByteBufferCapacity() {
        return 4;
    }

    @Override
    public ByteBuffer getPacketLengthByteBuffer() {
        return packetLengthByteBuffer;
    }

    @Override
    public int getPacketLength() {
        return packetLengthByteBuffer.getInt();
    }

    @Override
    public NetBuffer getInputBuffer() {
        return in.getBuffer();
    }

    public TransferInputStream getTransferInputStream(NetBuffer buffer) {
        // 没有读完一个包时会对全局buffer调用一次slice，此时生成一个新的buffer
        if (buffer != in.getBuffer())
            in.setBuffer(buffer);
        return in;
    }

    public TransferOutputStream getTransferOutputStream() {
        return out;
    }

    public TransferOutputStream getErrorTransferOutputStream() {
        return out;
    }

    public TransferOutputStream createTransferOutputStream(NetBuffer buffer) {
        return new TransferOutputStream(writableChannel, buffer);
    }

    protected void handleRequest(TransferInputStream in, int packetId, int packetType)
            throws IOException {
        throw DbException.getInternalError("handleRequest");
    }

    protected void handleResponse(TransferInputStream in, int packetId, int status) throws IOException {
        throw DbException.getInternalError("handleResponse");
    }

    protected void addAsyncCallback(int packetId, AsyncCallback<?> ac) {
        throw DbException.getInternalError("addAsyncCallback");
    }

    protected static DbException parseError(TransferInputStream in) {
        Throwable t;
        try {
            String sqlState = in.readString();
            String message = in.readString();
            String sql = in.readString();
            int errorCode = in.readInt();
            String stackTrace = in.readString();
            JdbcSQLException s = new JdbcSQLException(message, sql, sqlState, errorCode, null,
                    stackTrace);
            t = s;
            if (errorCode == ErrorCode.CONNECTION_BROKEN_1) {
                IOException e = new IOException(s.toString());
                e.initCause(s);
                t = e;
            }
        } catch (Exception e) {
            t = e;
        }
        return DbException.convert(t);
    }

    public void sendError(Session session, int packetId, Throwable t) {
        try {
            SQLException e = DbException.convert(t).getSQLException();
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            String trace = writer.toString();
            String message;
            String sql;
            if (e instanceof JdbcSQLException) {
                JdbcSQLException j = (JdbcSQLException) e;
                message = j.getOriginalMessage();
                sql = j.getSQL();
            } else {
                message = e.getMessage();
                sql = null;
            }
            TransferOutputStream out = getErrorTransferOutputStream();
            out.writeResponseHeader(session, packetId, Session.STATUS_ERROR);
            out.writeString(e.getSQLState()).writeString(message).writeString(sql)
                    .writeInt(e.getErrorCode()).writeString(trace).flush();
        } catch (Exception e2) {
            if (session != null)
                session.close();
            else if (writableChannel != null) {
                writableChannel.close();
            }
            logger.error("Failed to send error", e2);
        }
    }

    @Override
    public void handle(NetBuffer buffer) {
        TransferInputStream in = null;
        try {
            in = getTransferInputStream(buffer);
            boolean isRequest = in.readByte() == TransferOutputStream.REQUEST;
            int packetId = in.readInt();
            if (isRequest) {
                int packetType = in.readInt();
                handleRequest(in, packetId, packetType);
            } else {
                int status = in.readInt();
                handleResponse(in, packetId, status);
            }
        } catch (Throwable e) {
            if (isServer)
                logger.error("Failed to handle packet", e);
            else
                throw DbException.convert(e);
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }
}
