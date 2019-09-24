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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;

@SuppressWarnings("rawtypes")
public class AsyncCallback<T> {

    protected Transfer transfer;
    protected T result;
    protected DbException e;
    protected CountDownLatch latch = new CountDownLatch(1);

    protected AsyncHandler ah;
    protected boolean runEnd;

    public AsyncCallback() {
    }

    public AsyncCallback(AsyncHandler ah) {
        this.ah = ah;
    }

    public void setAsyncHandler(AsyncHandler ah) {
        this.ah = ah;
    }

    public void setTransfer(Transfer transfer) {
        this.transfer = transfer;
    }

    public void setDbException(DbException e) {
        this.e = e;
    }

    public void setDbException(DbException e, boolean cancel) {
        this.e = e;
        if (cancel)
            latch.countDown();
    }

    public void setResult(T result) {
        this.result = result;
    }

    public T getResult() {
        await();
        return result;
    }

    public void await() {
        await(-1);
    }

    public void await(long timeoutMillis) {
        try {
            if (timeoutMillis > 0)
                latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
            else
                latch.await();
            if (e != null)
                throw e;

            // 如果没有执行过run，抛出合适的异常
            if (!runEnd) {
                if (transfer != null && transfer.isClosed())
                    throw new RuntimeException("transfer is closed");
                else
                    throw new RuntimeException("time out");
            }
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        }
    }

    @SuppressWarnings("unchecked")
    public final void run(Transfer transfer) {
        // 放在最前面，不能放在最后面，
        // 否则调用了countDown，但是在设置runEnd为true前，调用await的线程读到的是false就会抛异常
        runEnd = true;
        if (e == null) {
            this.transfer.setDataInputStream(transfer.getDataInputStream());
            runInternal();
        } else if (ah != null) {
            AsyncResult r = new AsyncResult();
            r.setCause(e);
            ah.handle(r);
        }
        if (ah == null)
            latch.countDown();
    }

    protected void runInternal() {
    }
}
