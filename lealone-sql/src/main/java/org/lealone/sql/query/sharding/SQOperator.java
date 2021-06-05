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
package org.lealone.sql.query.sharding;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.SessionStatus;

//Sharding Query Operator
public class SQOperator {

    protected final SQCommand[] commands;
    protected final int maxRows;
    protected final CopyOnWriteArrayList<Result> results;
    protected final AtomicInteger resultCount;

    protected Result result;
    protected ServerSession session;

    protected volatile boolean end;
    protected volatile Throwable pendingException;

    // 确保只调用一次wakeUp
    private AtomicBoolean wakeUp;

    public SQOperator(SQCommand[] commands, int maxRows) {
        this.commands = commands;
        this.maxRows = maxRows;
        if (commands != null) {
            results = new CopyOnWriteArrayList<>();
            resultCount = new AtomicInteger(commands.length);
            wakeUp = new AtomicBoolean(false);
        } else {
            results = null;
            resultCount = null;
        }
    }

    public void setSession(ServerSession session) {
        this.session = session;
    }

    public void start() {
    }

    public void run() {
        if (!end && pendingException == null) {
            for (int i = 0, len = commands.length; i < len && pendingException == null; i++) {
                commands[i].executeDistributedQuery().onComplete(ar -> {
                    if (ar.isSucceeded()) {
                        results.add(ar.getResult());
                        if (resultCount.decrementAndGet() <= 0) {
                            end = true;
                            result = createFinalResult();
                        }
                    } else {
                        end = true;
                        pendingException = ar.getCause();
                    }

                    if (end && session != null && wakeUp != null && wakeUp.compareAndSet(false, true)) {
                        session.setStatus(SessionStatus.STATEMENT_COMPLETED);
                        session.getTransactionListener().wakeUp(); // 及时唤醒
                    }
                });
            }
        }
    }

    protected Result createFinalResult() {
        return null;
    }
}
