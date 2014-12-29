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
package org.lealone.command.router;

import java.util.List;

import org.lealone.command.FrontendCommand;

public class SQLRoutingInfo {
    //localRegion/remoteCommand与localRegions/remoteCommands只会二选一
    //localRegion与remoteCommand也只会二选一(或者localRegion为null，或者remoteCommand为null)
    //localRegions与remoteCommands有可能不同时为null

    /**
     * 此SQL涉及的数据在本地RegionServer的某个Region里
     */
    public String localRegion;

    /**
     * 此SQL涉及的数据在远程的某个RegionServer里, 建立一条到远程RegionServer的CommandRemote
     */
    public FrontendCommand remoteCommand;

    /**
     * 此SQL涉及的数据在本地RegionServer的多个Region里
     */
    public List<String> localRegions;

    /**
     * 此SQL涉及的数据在远程的多个RegionServer里, 建立多条到远程RegionServer的CommandRemote
     */
    public List<FrontendCommand> remoteCommands;
}
