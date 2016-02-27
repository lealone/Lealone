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

import java.util.ArrayList;
import java.util.HashSet;

import org.lealone.db.CommandParameter;
import org.lealone.db.result.Result;
import org.lealone.db.table.Table;

public interface Query {
    Result query(int maxRows);

    String getPlanSQL();

    boolean isEverything(ExpressionVisitor visitor);

    ArrayList<? extends CommandParameter> getParameters();

    void addGlobalCondition(CommandParameter param, int columnId, int comparisonType);

    void disableCache();

    double getCost();

    HashSet<Table> getTables();

    boolean allowGlobalConditions();

    int getColumnCount();

    ArrayList<? extends Expression> getExpressions();

    long getMaxDataModificationId();
}
