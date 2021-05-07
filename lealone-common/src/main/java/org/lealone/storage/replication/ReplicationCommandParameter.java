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
package org.lealone.storage.replication;

import java.util.List;

import org.lealone.db.CommandParameter;
import org.lealone.db.value.Value;

public class ReplicationCommandParameter implements CommandParameter {

    private final int index;
    private final int size;
    List<CommandParameter> commandParameters;

    public ReplicationCommandParameter(int index, List<CommandParameter> commandParameters) {
        this.index = index;
        this.size = commandParameters.size();
        this.commandParameters = commandParameters;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public void setValue(Value newValue, boolean closeOld) {
        for (int i = 0; i < size; i++) {
            commandParameters.get(i).setValue(newValue, closeOld);
        }
    }

    @Override
    public void setValue(Value value) {
        for (int i = 0; i < size; i++) {
            commandParameters.get(i).setValue(value);
        }
    }

    @Override
    public Value getValue() {
        return commandParameters.get(0).getValue();
    }

    @Override
    public void checkSet() {
        for (int i = 0; i < size; i++) {
            commandParameters.get(i).checkSet();
        }
    }

    @Override
    public boolean isValueSet() {
        return commandParameters.get(0).isValueSet();
    }

    @Override
    public int getType() {
        return commandParameters.get(0).getType();
    }

    @Override
    public long getPrecision() {
        return commandParameters.get(0).getPrecision();
    }

    @Override
    public int getScale() {
        return commandParameters.get(0).getScale();
    }

    @Override
    public int getNullable() {
        return commandParameters.get(0).getNullable();
    }
}
