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
package org.lealone.db.async;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.lealone.common.exceptions.DbException;
import org.lealone.net.NetInputStream;

@SuppressWarnings("rawtypes")
public class AsyncCallback<T> {

    protected T result;
    protected DbException e;
    protected CountDownLatch latch = new CountDownLatch(1);

    protected AsyncHandler ah;
    protected boolean runEnd;

    public AsyncCallback() {
    }

    public void setAsyncHandler(AsyncHandler ah) {
        this.ah = ah;
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
        return getResult(-1);
    }

    public T getResult(long timeoutMillis) {
        await(timeoutMillis);
        return result;
    }

    public void await() {
        await(-1);
    }

    public T await(long timeoutMillis) {
        try {
            if (timeoutMillis > 0)
                latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
            else
                latch.await();
            if (e != null)
                throw e;

            // 如果没有执行过run，抛出合适的异常
            if (!runEnd) {
                throw new RuntimeException("time out");
            }
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public final void run(NetInputStream in) {
        // 放在最前面，不能放在最后面，
        // 否则调用了countDown，但是在设置runEnd为true前，调用await的线程读到的是false就会抛异常
        runEnd = true;
        if (e == null) {
            try {
                runInternal(in);
            } catch (Throwable t) {
                e = DbException.convert(t);
            }
        } else if (ah != null) {
            AsyncResult r = new AsyncResult();
            r.setCause(e);
            ah.handle(r);
        }
        if (ah == null)
            latch.countDown();
    }

    protected void runInternal(NetInputStream in) throws Exception {
    }
}
