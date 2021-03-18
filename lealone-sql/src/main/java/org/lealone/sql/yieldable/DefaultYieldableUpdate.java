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

import org.lealone.common.exceptions.DbException;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.SessionStatus;
import org.lealone.sql.StatementBase;
import org.lealone.sql.router.SQLRouter;

public class DefaultYieldableUpdate extends YieldableUpdateBase {

    public DefaultYieldableUpdate(StatementBase statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        super(statement, asyncHandler);
        callStop = false;
    }

    @Override
    protected boolean executeInternal() {
        switch (session.getStatus()) {
        case TRANSACTION_NOT_START:
        case TRANSACTION_NOT_COMMIT:
        case STATEMENT_COMPLETED:
        case RETRYING:
            SQLRouter.executeUpdate(statement, ar -> handleResult(ar));
            break;
        case REPLICATION_COMPLETED:
            session.setStatus(SessionStatus.TRANSACTION_NOT_COMMIT);
            callStop = false;
            return false;
        }

        // 当前事务已经成功提交或当前语句已经执行完时就不必再让调度器轮循检查session的状态了
        return session.getStatus() != SessionStatus.STATEMENT_COMPLETED
                && session.getStatus() != SessionStatus.TRANSACTION_NOT_START;
    }

    private void handleResult(AsyncResult<Integer> ar) {
        if (ar.isSucceeded()) {
            // 返回的值为负数时，表示当前语句无法正常执行，需要等待其他事务释放锁
            if (ar.getResult() < 0) {
                // 在复制模式下执行时，可以把结果返回给客户端做冲突检测
                if (asyncHandler != null && session.needsHandleReplicationDbObjectLockConflict()) {
                    asyncHandler.handle(new AsyncResult<>(-1));
                } else {
                    session.setStatus(SessionStatus.WAITING); // 需要等待然后重新执行
                }
            } else {
                if (session.getReplicationName() != null) {
                    session.setStatus(SessionStatus.REPLICA_STATEMENT_COMPLETED);
                    if (asyncHandler != null)
                        asyncHandler.handle(new AsyncResult<>(ar.getResult()));
                    else
                        setResult(ar.getResult()); // 当前节点也是目标节点的场景
                } else {
                    session.setStatus(SessionStatus.STATEMENT_COMPLETED);
                    setResult(ar.getResult());
                    stop();
                }
            }
        } else {
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
            DbException e = DbException.convert(ar.getCause());
            handleException(e);
        }
    }
}
