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
package org.lealone.sql.router;

import org.lealone.db.result.ResultInterface;
import org.lealone.sql.ddl.DefineCommand;
import org.lealone.sql.dml.Delete;
import org.lealone.sql.dml.Insert;
import org.lealone.sql.dml.Merge;
import org.lealone.sql.dml.Select;
import org.lealone.sql.dml.Update;

public interface Router {

    int executeDefineCommand(DefineCommand defineCommand);

    int executeInsert(Insert insert);

    int executeMerge(Merge merge);

    int executeDelete(Delete delete);

    int executeUpdate(Update update);

    ResultInterface executeSelect(Select select, int maxRows, boolean scrollable);
}
