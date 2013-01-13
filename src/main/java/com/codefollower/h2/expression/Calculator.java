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
package com.codefollower.h2.expression;

import java.util.ArrayList;
import java.util.List;

import com.codefollower.h2.value.Value;

public class Calculator {
    private final Value[] currentRow;
    private final List<Value> result;
    private int index;

    public Calculator(Value[] currentRow) {
        this(currentRow, 0);
    }

    public Calculator(Value[] currentRow, int index) {
        this.currentRow = currentRow;
        this.index = index;
        this.result = new ArrayList<Value>(currentRow.length - 2);
    }

    public int getIndex() {
        return index;
    }

    public int addIndex() {
        return ++index;
    }

    public int addIndex(int i) {
        index += i;
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public List<Value> getResult() {
        return result;
    }

    public void addResultValue(Value v) {
        result.add(v);
    }

    public Value getValue(int i) {
        return currentRow[i];
    }
}
