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

public class DefaultYieldableShardingUpdate extends DefaultYieldableUpdate {

    public DefaultYieldableShardingUpdate(StatementBase statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        super(statement, asyncHandler);
    }

    @Override
    protected boolean executeInternal() {
        session.setStatus(SessionStatus.STATEMENT_RUNNING);
        SQLRouter.executeUpdate(statement, ar -> handleResult(ar));
        return yieldIfNeeded();
    }

    private void handleResult(AsyncResult<Integer> ar) {
        if (ar.isSucceeded()) {
            setResult(ar.getResult());
            // 返回的值为负数时，表示当前语句无法正常执行，需要等待其他事务释放锁
            if (ar.getResult() < 0) {
                session.setStatus(SessionStatus.WAITING);
            } else {
                session.setStatus(SessionStatus.STATEMENT_COMPLETED);
                stop();
            }
        } else {
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
            DbException e = DbException.convert(ar.getCause());
            handleException(e);
        }
    }
}
