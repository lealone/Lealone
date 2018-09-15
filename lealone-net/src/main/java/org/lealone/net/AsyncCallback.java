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

    public void setResult(T result) {
        this.result = result;
    }

    public T getResult() {
        await();
        return result;
    }

    public void await() {
        try {
            latch.await();
            if (e != null)
                throw e;
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        }
    }

    @SuppressWarnings("unchecked")
    public final void run(Transfer transfer) {
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
