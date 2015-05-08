/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.lealone.mvdb.engine;

import org.lealone.engine.Database;
import org.lealone.engine.Session;
import org.lealone.mvdb.dbobject.table.MVTable;
import org.lealone.mvstore.type.DataType;

public interface TransactionStorageEngine {
    /**
     * Check whether a given map exists.
     *
     * @param name the map name
     * @return true if it exists
     */
    public boolean hasMap(Database db, String name);

    public boolean isInMemory(Database db);

    public void removeTable(MVTable table);

    public String nextTemporaryMapName(Database db);

    public <K, V> TransactionMap<K, V> openMap(Session session, String name, DataType keyType, DataType valueType);
}
