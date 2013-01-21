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
package com.codefollower.yourbase.expression;

import com.codefollower.yourbase.expression.Comparison;
import com.codefollower.yourbase.value.Value;

public class ConditionInfo {
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    protected final String conditionName;
    protected int compareTypeStart = -1;
    protected Value compareValueStart;
    protected int compareTypeStop = -1;
    protected Value compareValueStop;

    public ConditionInfo(String conditionName) {
        this.conditionName = conditionName;
    }

    public String getConditionName() {
        return conditionName;
    }

    public int getCompareTypeStart() {
        return compareTypeStart;
    }

    public void setCompareTypeStart(int compareTypeStart) {
        this.compareTypeStart = compareTypeStart;
    }

    public Value getCompareValueStart() {
        return compareValueStart;
    }

    public void setCompareValueStart(Value compareValueStart) {
        this.compareValueStart = compareValueStart;
    }

    public int getCompareTypeStop() {
        return compareTypeStop;
    }

    public void setCompareTypeStop(int compareTypeStop) {
        this.compareTypeStop = compareTypeStop;
    }

    public Value getCompareValueStop() {
        return compareValueStop;
    }

    public void setCompareValueStop(Value compareValueStop) {
        this.compareValueStop = compareValueStop;
    }

    public String[] getConditionValues() {
        String[] conditionValues = new String[2];
        if (getCompareValueStart() != null)
            conditionValues[0] = getCompareValueStart().getString();
        if (getCompareValueStop() != null)
            conditionValues[1] = getCompareValueStop().getString();
        return conditionValues;
    }

    public String getConditionValue() {
        String conditionValue = null;

        if (getCompareTypeStart() != Comparison.EQUAL)
            throw new RuntimeException(conditionName + " compare type is not '='");
        if (getCompareValueStart() != null)
            conditionValue = getCompareValueStart().getString();

        return conditionValue;
    }
}
