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
package org.lealone.sql.yieldable;

import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.sql.StatementBase;
import org.lealone.transaction.Transaction;

public abstract class YieldableListenableUpdateBase extends YieldableUpdateBase implements Transaction.Listener {

    protected final AtomicInteger pendingOperationCounter = new AtomicInteger();
    protected volatile RuntimeException pendingOperationException;
    protected boolean loopEnd;

    public YieldableListenableUpdateBase(StatementBase statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        super(statement, asyncHandler);

        // 执行操作时都是异步的，
        // 当所有异步操作完成时才能调用stop方法给客户端发回响应结果
        callStop = false;
    }

    @Override
    protected boolean executeInternal() {
        if (!loopEnd) {
            if (executeUpdate()) {
                if (asyncHandler != null && session.needsHandleReplicationRowLockConflict()) {
                    asyncHandler.handle(new AsyncResult<>(-1));
                }
                return true;
            }
        }
        if (loopEnd) {
            if (pendingOperationException != null)
                throw pendingOperationException;
            if (pendingOperationCounter.get() <= 0) {
                setResult(affectedRows);
                callStop = true;
                return false;
            }
        }
        return true;
    }

    protected abstract boolean executeUpdate();

    @Override
    public void beforeOperation() {
        pendingOperationCounter.incrementAndGet();
    }

    @Override
    public void operationUndo() {
        pendingOperationCounter.decrementAndGet();
    }

    @Override
    public void operationComplete() {
        pendingOperationCounter.decrementAndGet();
    }

    @Override
    public void setException(RuntimeException e) {
        pendingOperationException = e;
    }
}
