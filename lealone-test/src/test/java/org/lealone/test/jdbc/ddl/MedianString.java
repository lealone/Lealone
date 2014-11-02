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
package org.lealone.test.jdbc.ddl;

import java.sql.Connection;
import java.sql.Types;
import java.util.ArrayList;

import org.lealone.api.AggregateFunction;

public class MedianString implements AggregateFunction {

    private ArrayList<String> list = new ArrayList<String>();

    public void add(Object value) {
        list.add(value.toString());
    }

    public Object getResult() {
        return list.get(list.size() / 2);
    }

    public int getType(int[] inputType) {
        return Types.VARCHAR;
    }

    public void init(Connection conn) {
        // nothing to do
    }

}
