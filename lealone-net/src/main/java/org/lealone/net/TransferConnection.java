/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.net;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.exceptions.JdbcSQLException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.Session;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.async.AsyncTaskHandler;

public abstract class TransferConnection extends AsyncConnection {

    private static final Logger logger = LoggerFactory.getLogger(TransferConnection.class);

    private NetBuffer lastBuffer;

    public TransferConnection(WritableChannel writableChannel, boolean isServer) {
        super(writableChannel, isServer);
    }

    protected void handleRequest(Transfer transfer, int id, int operation) throws IOException {
        throw DbException.throwInternalError("handleRequest");
    }

    protected void handleResponse(Transfer transfer, int id, int status) throws IOException {
        throw DbException.throwInternalError("handleResponse");
    }

    protected void addAsyncCallback(int id, AsyncCallback<?> ac) {
        throw DbException.throwInternalError("addAsyncCallback");
    }

    protected static DbException parseError(Transfer transfer) {
        Throwable t;
        try {
            String sqlstate = transfer.readString();
            String message = transfer.readString();
            String sql = transfer.readString();
            int errorCode = transfer.readInt();
            String stackTrace = transfer.readString();
            JdbcSQLException s = new JdbcSQLException(message, sql, sqlstate, errorCode, null, stackTrace);
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

    public void sendError(Transfer transfer, int id, Throwable t) {
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

            // if (isServer) {
            // message = "[Server] " + message;
            // }

            transfer.reset(); // 为什么要reset? 见reset中的注释

            transfer.writeResponseHeader(id, Session.STATUS_ERROR);
            transfer.writeString(e.getSQLState()).writeString(message).writeString(sql).writeInt(e.getErrorCode())
                    .writeString(trace).flush();
        } catch (Exception e2) {
            if (transfer.getSession() != null)
                transfer.getSession().close();
            else if (writableChannel != null) {
                writableChannel.close();
            }
            logger.error("Failed to send error", e2);
        }
    }

    @Override
    public void handle(NetBuffer buffer) {
        if (lastBuffer != null) {
            buffer = lastBuffer.appendBuffer(buffer);
            lastBuffer = null;
        }

        int length = buffer.length();
        if (length < 4) {
            lastBuffer = buffer;
            return;
        }

        int pos = 0;
        try {
            while (true) {
                // 必须生成新的Transfer实例，不同协议包对应不同Transfer实例，
                // 否则如果有多个CommandHandler线程时会用同一个Transfer实例写数据，这会产生并发问题。
                Transfer transfer;
                if (pos == 0)
                    transfer = new Transfer(this, writableChannel, buffer);
                else
                    transfer = new Transfer(this, writableChannel, buffer.slice(pos, pos + length));
                int packetLength = transfer.readInt();
                if (length - 4 == packetLength) {
                    handlePacket(transfer);
                    break;
                } else if (length - 4 > packetLength) {
                    handlePacket(transfer);
                    pos = pos + packetLength + 4;
                    length = length - (packetLength + 4);
                    // 有可能剩下的不够4个字节了
                    if (length < 4) {
                        lastBuffer = buffer.getBuffer(pos, pos + length);
                        break;
                    } else {
                        continue;
                    }
                } else {
                    lastBuffer = buffer.getBuffer(pos, pos + length);
                    break;
                }
            }
        } catch (Throwable e) {
            if (isServer)
                logger.error("Failed to handle packet", e);
        }
    }

    public AsyncTaskHandler getAsyncTaskHandler() {
        return null;
    }

    private void handlePacket(Transfer transfer) throws IOException {
        boolean isRequest = transfer.readByte() == Transfer.REQUEST;
        int id = transfer.readInt();
        AsyncTask task;
        if (isRequest) {
            int operation = transfer.readInt();
            task = new RequestPacketDeliveryTask(this, transfer, id, operation);
        } else {
            int status = transfer.readInt();
            task = new ResponsePacketDeliveryTask(this, transfer, id, status);
        }
        AsyncTaskHandler handler = getAsyncTaskHandler();
        if (handler == null) {
            task.run();
        } else {
            handler.handle(task);
        }
    }

    private static abstract class PacketDeliveryTask implements AsyncTask {
        final TransferConnection conn;
        final Transfer transfer;
        final int id;

        public PacketDeliveryTask(TransferConnection conn, Transfer transfer, int id) {
            this.conn = conn;
            this.transfer = transfer;
            this.id = id;
        }

        @Override
        public int getPriority() {
            return NORM_PRIORITY;
        }
    }

    private static class RequestPacketDeliveryTask extends PacketDeliveryTask {
        final int operation;

        public RequestPacketDeliveryTask(TransferConnection conn, Transfer transfer, int id, int operation) {
            super(conn, transfer, id);
            this.operation = operation;
        }

        @Override
        public void run() {
            try {
                conn.handleRequest(transfer, id, operation);
            } catch (Throwable e) {
                logger.error("Failed to handle request, id: " + id + ", operation: " + operation, e);
                conn.sendError(transfer, id, e);
            }
        }
    }

    private static class ResponsePacketDeliveryTask extends PacketDeliveryTask {
        final int status;

        public ResponsePacketDeliveryTask(TransferConnection conn, Transfer transfer, int id, int status) {
            super(conn, transfer, id);
            this.status = status;
        }

        @Override
        public void run() {
            try {
                conn.handleResponse(transfer, id, status);
            } catch (Throwable e) {
                // String msg = "Failed to handle response, id: " + id + ", status: " + status;
                throw DbException.convert(e);
            }
        }
    }
}
