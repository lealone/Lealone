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
package org.h2.util;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;

public class HBaseRegionInfo {
    private final HRegionLocation regionLocation;
    private final HRegionInfo regionInfo;
    private final String regionName;
    private final String regionServerURL;

    public HBaseRegionInfo(HRegionLocation regionLocation) {
        this.regionLocation = regionLocation;
        this.regionInfo = regionLocation.getRegionInfo();
        this.regionName = regionLocation.getRegionInfo().getRegionNameAsString();
        this.regionServerURL = HBaseUtils.createURL(regionLocation);
    }

    public HRegionInfo getHRegionInfo() {
        return regionInfo;
    }

    public String getRegionName() {
        return regionName;
    }

    public String getRegionServerURL() {
        return regionServerURL;
    }

    public String getHostname() {
        return regionLocation.getHostname();
    }

    public int getH2TcpPort() {
        return regionLocation.getH2TcpPort();
    }
}
