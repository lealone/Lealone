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
package org.lealone.sql.query;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.db.result.ResultTarget;
import org.lealone.db.session.SessionStatus;
import org.lealone.sql.yieldable.YieldableQueryBase;

class YieldableSelect extends YieldableQueryBase {

    private final Select select;
    private final Select statement;
    private final ResultTarget target;

    public YieldableSelect(Select select, Select statement, int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler, ResultTarget target) {
        super(statement, maxRows, scrollable, asyncHandler);
        this.select = select;
        this.statement = statement;
        this.target = target;
    }

    @Override
    protected boolean startInternal() {
        this.select.fireBeforeSelectTriggers();
        this.select.resultCache.getResult(maxRows, target, async);
        return false;
    }

    @Override
    protected void stopInternal() {
        statement.queryOperator.stop();
    }

    @Override
    protected void executeInternal() {
        if (this.select.resultCache.useCache) {
            this.select.resultCache.lastResult.reset();
            setResult(this.select.resultCache.lastResult, this.select.resultCache.lastResult.getRowCount());
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
            return;
        }
        if (query()) {
            return;
        }
        if (statement.queryOperator.target != null) {
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
            return;
        }
        if (statement.queryOperator.localResult != null) {
            setResult(statement.queryOperator.localResult, statement.queryOperator.localResult.getRowCount());
            this.select.resultCache.lastResult = statement.queryOperator.localResult;
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
            return;
        }
        return;
    }

    private boolean query() {
        statement.queryOperator.run();
        if (statement.queryOperator.loopEnd)
            return false;
        return true;
    }
}
