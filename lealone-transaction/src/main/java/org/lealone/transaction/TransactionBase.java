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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.lealone.command.router.FrontendSessionPool;
import org.lealone.engine.FrontendSession;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.util.New;

public abstract class TransactionBase implements TransactionInterface {

    protected static final ExecutorService executorService = Executors.newCachedThreadPool();

    protected Session session;
    protected long transactionId;
    protected String transactionName;
    protected boolean autoCommit;
    protected long commitTimestamp;

    //协调者或参与者自身的本地事务名
    protected StringBuilder localTransactionNamesBuilder;
    //如果本事务是协调者中的事务，那么在此字段中存放其他参与者的本地事务名
    protected final ConcurrentSkipListSet<String> participantLocalTransactionNames = new ConcurrentSkipListSet<String>();

    protected TransactionBase() {
    }

    protected TransactionBase(Session session) {
        setSession(session);
    }

    public void setSession(Session session) {
        session.setTransaction(this);
        this.session = session;
        autoCommit = session.getAutoCommit();
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    public String getTransactionName() {
        return transactionName;
    }

    public String getAllLocalTransactionNames() {
        getLocalTransactionNames();
        return localTransactionNamesBuilder.toString();
    }

    /**
     * 假设有RS1、RS2、RS3，Client启动的一个事务涉及这三个RS, 
     * 第一个接收到Client读写请求的RS即是协调者也是参与者，之后Client的任何读写请求都只会跟协调者打交道，
     * 假设这里的协调者是RS1，当读写由RS1转发到RS2时，RS2在完成读写请求后会把它的本地事务名(可能有多个(嵌套事务)发回来，
     * 此时协调者必须记下所有其他参与者的本地事务名。<p>
     * 
     * 如果本地事务名是null，代表参与者执行完读写请求后发现跟上次的本地事务名一样，为了减少网络传输就不再重发。
     */
    @Override
    public void addLocalTransactionNames(String localTransactionNames) {
        if (localTransactionNames != null) {
            for (String name : localTransactionNames.split(","))
                participantLocalTransactionNames.add(name.trim());
        }
    }

    @Override
    public String getLocalTransactionNames() {
        if (transactionName == null)
            transactionName = TransactionManager.getHostAndPort() + ":" + transactionId;
        StringBuilder buff = new StringBuilder(transactionName);

        if (!participantLocalTransactionNames.isEmpty()) {
            for (String name : participantLocalTransactionNames) {
                buff.append(',');
                buff.append(name);
            }
        }

        if (localTransactionNamesBuilder != null && localTransactionNamesBuilder.equals(buff))
            return null;
        localTransactionNamesBuilder = buff;
        return buff.toString();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    protected void endTransaction() {
        if (!session.getFrontendSessionCache().isEmpty()) {
            for (FrontendSession fs : session.getFrontendSessionCache().values()) {
                fs.setTransaction(null);
                FrontendSessionPool.release(fs);
            }

            session.getFrontendSessionCache().clear();
        }

        if (!session.isRoot())
            session.setAutoCommit(true);
    }

    protected List<Future<Void>> parallelCommitOrRollback(final String allLocalTransactionNames) {
        int size = session.getFrontendSessionCache().size();
        List<Future<Void>> futures = New.arrayList(size);
        for (final FrontendSession fs : session.getFrontendSessionCache().values()) {
            futures.add(executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (allLocalTransactionNames != null)
                        fs.commitTransaction(allLocalTransactionNames);
                    else
                        fs.rollbackTransaction();
                    return null;
                }
            }));
        }
        return futures;
    }

    protected void waitFutures(List<Future<Void>> futures) {
        try {
            for (int i = 0, size = futures.size(); i < size; i++) {
                futures.get(i).get();
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void commit() {
        commit(null);
    }

    @Override
    public void commit(String allLocalTransactionNames) {
        try {
            if (allLocalTransactionNames == null)
                allLocalTransactionNames = getAllLocalTransactionNames();
            List<Future<Void>> futures = null;
            if (!isAutoCommit() && session.getFrontendSessionCache().size() > 0)
                futures = parallelCommitOrRollback(allLocalTransactionNames);

            commitLocal(allLocalTransactionNames);
            if (futures != null)
                waitFutures(futures);
        } finally {
            endTransaction();
        }
    }

    protected abstract void commitLocal(String allLocalTransactionNames);
}
