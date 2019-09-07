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
package org.lealone.storage.aose;

import org.lealone.db.DataHandler;
import org.lealone.storage.LobStorage;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageBuilder;
import org.lealone.storage.StorageEngineBase;
import org.lealone.storage.aose.lob.AOLobStorage;

public class AOStorageEngine extends StorageEngineBase {

    public static final String NAME = "AOSE";

    public AOStorageEngine() {
        super(NAME);
    }

    @Override
    public StorageBuilder getStorageBuilder() {
        return new AOStorageBuilder(config);
    }

    @Override
    public LobStorage getLobStorage(DataHandler dataHandler, Storage storage) {
        return new AOLobStorage(dataHandler, storage);
    }
}
