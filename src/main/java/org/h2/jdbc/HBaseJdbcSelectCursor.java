/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.h2.jdbc;

import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.SearchRow;

public class HBaseJdbcSelectCursor implements Cursor {
    //private final Session session;

    private ResultInterface result;

    public HBaseJdbcSelectCursor(ResultInterface result, Session session, SearchRow first, SearchRow last) {
        //this.session = session;
        this.result = result;
    }

    @Override
    public Row get() {
        return new Row(result.currentRow(), -1);
    }

    @Override
    public SearchRow getSearchRow() {
        return get();
    }

    @Override
    public boolean next() {
        return result.next();
    }

    @Override
    public boolean previous() {
        return false;
    }

}
