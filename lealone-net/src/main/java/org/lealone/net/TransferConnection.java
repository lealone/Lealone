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
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.exceptions.JdbcSQLException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.Session;
import org.lealone.db.api.ErrorCode;

public abstract class TransferConnection extends AsyncConnection {

    private static final Logger logger = LoggerFactory.getLogger(TransferConnection.class);

    protected final ConcurrentHashMap<Integer, AsyncCallback<?>> callbackMap = new ConcurrentHashMap<>();
    private NetBuffer lastBuffer;

    public TransferConnection(WritableChannel writableChannel, boolean isServer) {
        super(writableChannel, isServer);
    }

    protected abstract void handleRequest(Transfer transfer, int id, int operation) throws IOException;

    protected void handleResponse(Transfer transfer, int id, int status) throws IOException {
        DbException e = null;
        if (status == Session.STATUS_OK) {
            // ok
        } else if (status == Session.STATUS_ERROR) {
            e = parseError(transfer);
        } else if (status == Session.STATUS_CLOSED) {
            transfer = null;
        } else {
            e = DbException.get(ErrorCode.CONNECTION_BROKEN_1, "unexpected status " + status);
        }

        AsyncCallback<?> ac = callbackMap.remove(id);
        if (ac == null) {
            String msg = "Async callback is null, may be a bug! id = " + id;
            if (e != null) {
                logger.warn(msg, e);
            } else {
                logger.warn(msg);
            }
            return;
        }
        if (e != null)
            ac.setDbException(e);
        ac.run(transfer);
    }

    public void addAsyncCallback(int id, AsyncCallback<?> ac) {
        callbackMap.put(id, ac);
    }

    public AsyncCallback<?> getAsyncCallback(int id) {
        return callbackMap.get(id);
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

    protected void sendError(Transfer transfer, int id, Throwable t) {
        // logger.error("sendError", t);
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
                    parsePacket(transfer);
                    break;
                } else if (length - 4 > packetLength) {
                    parsePacket(transfer);
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
                logger.error("Parse packet", e);
        }
    }

    private void parsePacket(Transfer transfer) throws IOException {
        boolean isRequest = transfer.readByte() == Transfer.REQUEST;
        int id = transfer.readInt();
        if (isRequest) {
            int operation = transfer.readInt();
            try {
                handleRequest(transfer, id, operation);
            } catch (Throwable e) {
                logger.error("Failed to handle request, operation: " + operation, e);
                sendError(transfer, id, e);
            }
        } else {
            int status = transfer.readInt();
            handleResponse(transfer, id, status);
        }
    }
}
