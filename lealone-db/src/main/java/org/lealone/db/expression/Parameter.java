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
package org.lealone.db.expression;

import org.lealone.common.message.DbException;
import org.lealone.common.value.Value;
import org.lealone.common.value.ValueNull;
import org.lealone.db.ParameterInterface;
import org.lealone.db.Session;

public class Parameter implements ParameterInterface {
    private final int index;
    private Value value;

    public Parameter(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public Value getParamValue() {
        if (value == null) {
            // to allow parameters in function tables
            return ValueNull.INSTANCE;
        }
        return value;
    }

    public Value getValue(Session session) {
        return getParamValue();
    }

    @Override
    public void setValue(Value v, boolean closeOld) {
        // don't need to close the old value as temporary files are anyway removed
        this.value = v;
    }

    public void setValue(Value v) {
        this.value = v;
    }

    @Override
    public void checkSet() throws DbException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isValueSet() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getPrecision() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getScale() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getNullable() {
        // TODO Auto-generated method stub
        return 0;
    }
}
