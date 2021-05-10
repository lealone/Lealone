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

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.SessionStatus;
import org.lealone.sql.StatementBase;

public class DefaultYieldableReplicationUpdate extends DefaultYieldableUpdate {

    public DefaultYieldableReplicationUpdate(StatementBase statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        super(statement, asyncHandler);
    }

    @Override
    protected void executeInternal() {
        switch (session.getStatus()) {
        case TRANSACTION_NOT_START:
        case TRANSACTION_NOT_COMMIT:
        case STATEMENT_COMPLETED:
        case RETRYING_RETURN_RESULT: // 重试返回结果后还不能立刻结束，所以也需要转到STATEMENT_RUNNING状态
            session.setStatus(SessionStatus.STATEMENT_RUNNING);
        case RETRYING: // 重试不用返回结果，立即结束
            executeUpdate();
            break;
        }
    }

    private void executeUpdate() {
        int updateCount = statement.update();
        setResult(updateCount);

        // 返回的值为负数时，表示当前语句无法正常执行，需要等待其他事务释放锁
        if (updateCount < 0) {
            session.setStatus(SessionStatus.WAITING);
            // 在复制模式下执行时，可以把结果返回给客户端做冲突检测
            if (asyncHandler != null && session.needsHandleReplicationDbObjectLockConflict()) {
                asyncHandler.handle(asyncResult);
            }
        } else {
            // 发生复制冲突时当前session进行重试，此时已经不需要再向客户端返回结果了，直接提交即可
            if (session.getStatus() == SessionStatus.RETRYING) {
                stop();
            } else {
                // 此时语句还没有完成，需要等到执行session.handleReplicaConflict后才完成
                if (asyncHandler != null) {
                    AsyncResult<Integer> ar = asyncResult;
                    asyncResult = null; // 避免发送第二次
                    asyncHandler.handle(ar);
                }
            }
        }
    }
}
