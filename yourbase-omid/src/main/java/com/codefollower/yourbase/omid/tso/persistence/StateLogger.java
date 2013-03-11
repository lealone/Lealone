/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. See accompanying LICENSE file.
 */

package com.codefollower.yourbase.omid.tso.persistence;

import com.codefollower.yourbase.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.codefollower.yourbase.omid.tso.persistence.LoggerAsyncCallback.LoggerInitCallback;

public interface StateLogger {

    /**
     * Initializes the storage subsystem to add
     * new records.
     * 
     * @param cb
     * @param ctx
     */
    void initialize(LoggerInitCallback cb, Object ctx) throws LoggerException;

    /**
     * Add a new record.
     * 
     * @param record
     * @param cb
     * @param ctx
     */
    void addRecord(byte[] record, AddRecordCallback cb, Object ctx);

    /**
     * Shut down logger.
     */
    void shutdown();

}
