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
package org.lealone.db.value;

import java.util.List;

import org.lealone.common.trace.Trace;

/**
 * Represents a readonly ARRAY value.
 */
public class ReadonlyArray extends ArrayBase {
    {
        this.trace = Trace.NO_TRACE;
    }

    public ReadonlyArray(Value value) {
        this.value = value;
    }

    public ReadonlyArray(String value) {
        setValue(value);
    }

    @SuppressWarnings("unchecked")
    public ReadonlyArray(Object value) {
        if (value instanceof List) {
            setValue((List<String>) value);
        } else {
            setValue(value.toString());
        }
    }

    public ReadonlyArray(List<String> list) {
        setValue(list);
    }

    private void setValue(String value) {
        this.value = ValueString.get(value);
    }

    private void setValue(List<String> list) {
        int size = list.size();
        Value[] values = new Value[size];
        for (int i = 0; i < size; i++) {
            values[i] = ValueString.get(list.get(i));
        }
        this.value = ValueArray.get(values);
    }
}
