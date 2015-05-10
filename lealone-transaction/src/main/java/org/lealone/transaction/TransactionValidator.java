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
package org.lealone.transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.lealone.engine.Constants;

class TransactionValidator extends Thread {

    private static final QueuedMessage CLOSE_SENTINEL = new QueuedMessage(null, null, null);

    private static final TransactionValidator INSTANCE = new TransactionValidator();

    public static TransactionValidator getInstance() {
        return INSTANCE;
    }

    private final BlockingQueue<QueuedMessage> backlog = new LinkedBlockingQueue<>();

    private volatile boolean isStopped = false;

    public TransactionValidator() {
        super("TransactionValidator");
    }

    public void close() {
        backlog.clear();
        isStopped = true;
        backlog.add(CLOSE_SENTINEL);
    }

    @Override
    public void run() {
        // keeping list (batch) size small for now; that way we don't have an unbounded array (that we never resize)
        final List<QueuedMessage> drainedMessages = new ArrayList<>(128);
        outer: while (true) {
            if (backlog.drainTo(drainedMessages, drainedMessages.size()) == 0) {
                try {
                    drainedMessages.add(backlog.take());
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }

            for (QueuedMessage qm : drainedMessages) {
                if (isStopped)
                    break outer;
                try {
                    validateTransaction(qm);
                } catch (Exception e) {
                    //e.printStackTrace();
                }
            }
            drainedMessages.clear();
        }
    }

    private void validateTransaction(QueuedMessage qm) {
        String[] allLocalTransactionNames = qm.allLocalTransactionNames.split(",");
        boolean isFullSuccessful = true;

        for (String localTransactionName : allLocalTransactionNames) {
            if (!localTransactionName.startsWith(qm.transactionEngine.hostAndPort)) {
                if (!qm.t.validator.validateTransaction(localTransactionName)) {
                    isFullSuccessful = false;
                    break;
                }
            }
        }

        if (isFullSuccessful) {
            qm.transactionEngine.commitAfterValidate(qm.t.transactionId);
        }
    }

    public void enqueue(MVCCTransactionEngine transactionEngine, MVCCTransaction t, String allLocalTransactionNames) {
        try {
            backlog.put(new QueuedMessage(transactionEngine, t, allLocalTransactionNames));
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    private static class QueuedMessage {
        final MVCCTransactionEngine transactionEngine;
        final MVCCTransaction t;
        final String allLocalTransactionNames;

        QueuedMessage(MVCCTransactionEngine transactionEngine, MVCCTransaction t, String allLocalTransactionNames) {
            this.transactionEngine = transactionEngine;
            this.t = t;
            this.allLocalTransactionNames = allLocalTransactionNames;
        }
    }

    static String createURL(String dbName, String host, String port) {
        StringBuilder url = new StringBuilder(100);
        url.append(Constants.URL_PREFIX).append(Constants.URL_TCP).append("//");
        url.append(host).append(":").append(port);
        url.append("/").append(dbName);
        return url.toString();
    }
}
