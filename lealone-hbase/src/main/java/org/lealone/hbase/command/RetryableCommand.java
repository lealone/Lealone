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
package org.lealone.hbase.command;

import java.io.IOException;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.lealone.api.ErrorCode;
import org.lealone.command.CommandContainer;
import org.lealone.command.Parser;
import org.lealone.command.Prepared;
import org.lealone.hbase.util.HBaseUtils;
import org.lealone.message.DbException;
import org.lealone.message.JdbcSQLException;
import org.lealone.result.ResultInterface;

public class RetryableCommand extends CommandContainer {
    private final long pause;
    private final int numRetries;

    protected RetryableCommand(Parser parser, String sql, Prepared prepared) {
        super(parser, sql, prepared);

        pause = HBaseUtils.getConfiguration().getLong(HConstants.HBASE_CLIENT_PAUSE, HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
        numRetries = HBaseUtils.getConfiguration().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    }

    @Override
    protected int updateInternal() {
        return ((Integer) execute(null)).intValue();
    }

    @Override
    protected ResultInterface queryInternal(int maxrows) {
        return (ResultInterface) execute(maxrows);
    }

    //改编自org.apache.hadoop.hbase.client.ServerCallable.withRetries()
    private Object execute(Integer maxrows) {
        Throwable cause = null;
        for (int tries = 0; tries < numRetries; tries++) {
            try {
                if (maxrows == null)
                    return Integer.valueOf(prepared.update());
                else
                    return prepared.query(maxrows.intValue());
            } catch (Throwable t) {
                if (t instanceof DoNotRetryIOException) {
                    throw DbException.convert(t);
                } else if (t instanceof DbException) {
                    DbException dbe = (DbException) t;
                    if (dbe.getCause() instanceof JdbcSQLException) {
                        if (((JdbcSQLException) dbe.getCause()).getCause() instanceof DoNotRetryIOException) {
                            throw dbe;
                        }
                    }
                    if (dbe.getErrorCode() != ErrorCode.IO_EXCEPTION_1)
                        throw dbe;
                } else if (t instanceof RuntimeException) {
                    if (((RuntimeException) t).getCause() instanceof DoNotRetryIOException) {
                        throw ((RuntimeException) t);
                    }
                }
                session.rollback();
                try {
                    HBaseUtils.getConnection().clearRegionCache();
                } catch (IOException e) {
                    throw DbException.convert(e);
                }
                cause = t;
            }
            try {
                Thread.sleep(ConnectionUtils.getPauseTime(pause, tries));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw DbException.convert(new IOException("Giving up after tries=" + tries, e));
            }
        }
        throw DbException.convert(new RetriesExhaustedException(cause));
    }

    private static class RetriesExhaustedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public RetriesExhaustedException(Throwable cause) {
            super(cause);
        }
    }
}
