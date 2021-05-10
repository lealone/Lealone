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

public abstract class DefaultYieldableUpdate extends YieldableUpdateBase {

    public DefaultYieldableUpdate(StatementBase statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        super(statement, asyncHandler);
    }

    protected boolean yieldIfNeeded() {
        // 当前事务已经成功提交或当前语句已经执行完时就不必再让调度器轮循检查session的状态了
        return session.getStatus() != SessionStatus.STATEMENT_COMPLETED
                && session.getStatus() != SessionStatus.TRANSACTION_NOT_START;
    }
}
