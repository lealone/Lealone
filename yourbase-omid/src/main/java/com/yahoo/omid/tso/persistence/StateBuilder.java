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

package com.yahoo.omid.tso.persistence;

import com.yahoo.omid.tso.TSOState;

/**
 * This is an interface for mechanisms that implement
 * the recovery of the TSO state. 
 *
 */

public abstract class StateBuilder {

    /**
     * Logger protocol object. Implements the logic to execute 
     * state taken out of log records.
     */
    LoggerProtocol protocol;

    /**
     * This call should create a new TSOState object and populate
     * it accordingly. If there was an incarnation of TSO in the past,
     * then this call recovers the state to populate the TSOState
     * object.
     * 
     * 
     * @return a new TSOState
     */
    abstract TSOState buildState() throws LoggerException;

    /**
     * Shuts down state builder.
     */
    abstract void shutdown();

}
