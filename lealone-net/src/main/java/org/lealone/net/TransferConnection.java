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
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.session.Session;

public abstract class TransferConnection extends AsyncConnection {

    private static final Logger logger = LoggerFactory.getLogger(TransferConnection.class);

    private NetBuffer lastBuffer;

    public TransferConnection(WritableChannel writableChannel, boolean isServer) {
        super(writableChannel, isServer);
    }

    public TransferOutputStream createTransferOutputStream(Session session) {
        return new TransferOutputStream(this, session, writableChannel);
    }

    protected void handleRequest(TransferInputStream in, int packetId, int packetType) throws IOException {
        throw DbException.throwInternalError("handleRequest");
    }

    protected void handleResponse(TransferInputStream in, int packetId, int status) throws IOException {
        throw DbException.throwInternalError("handleResponse");
    }

    protected void addAsyncCallback(int packetId, AsyncCallback<?> ac) {
        throw DbException.throwInternalError("addAsyncCallback");
    }

    protected static DbException parseError(TransferInputStream in) {
        Throwable t;
        try {
            String sqlstate = in.readString();
            String message = in.readString();
            String sql = in.readString();
            int errorCode = in.readInt();
            String stackTrace = in.readString();
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

    public void sendError(Session session, int packetId, Throwable t) {
        try {
            TransferOutputStream out = createTransferOutputStream(session);
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

            out.writeResponseHeader(packetId, Session.STATUS_ERROR);
            out.writeString(e.getSQLState()).writeString(message).writeString(sql).writeInt(e.getErrorCode())
                    .writeString(trace).flush();
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
                TransferInputStream in;
                if (pos == 0)
                    in = new TransferInputStream(buffer);
                else
                    in = new TransferInputStream(buffer.slice(pos, pos + length));
                int packetLength = in.readInt();
                if (length - 4 == packetLength) {
                    handlePacket(in);
                    break;
                } else if (length - 4 > packetLength) {
                    handlePacket(in);
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
            else
                throw DbException.convert(e);
        }
    }

    private void handlePacket(TransferInputStream in) throws IOException {
        boolean isRequest = in.readByte() == TransferOutputStream.REQUEST;
        int packetId = in.readInt();
        if (isRequest) {
            int packetType = in.readInt();
            handleRequest(in, packetId, packetType);
        } else {
            int status = in.readInt();
            handleResponse(in, packetId, status);
        }
    }
}
