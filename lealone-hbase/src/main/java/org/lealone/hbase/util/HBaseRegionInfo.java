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
package org.lealone.hbase.util;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.lealone.hbase.zookeeper.ZooKeeperAdmin;

public class HBaseRegionInfo {
    private final HRegionLocation regionLocation;
    private final HRegionInfo regionInfo;
    private final String regionName;
    private final byte[] regionNameAsBytes;
    private final String regionServerURL;

    public HBaseRegionInfo(HRegionLocation regionLocation) {
        this.regionLocation = regionLocation;
        this.regionInfo = regionLocation.getRegionInfo();
        this.regionName = regionLocation.getRegionInfo().getRegionNameAsString();
        this.regionNameAsBytes = regionLocation.getRegionInfo().getRegionName();
        this.regionServerURL = HBaseUtils.createURL(regionLocation);
    }

    public HRegionInfo getHRegionInfo() {
        return regionInfo;
    }

    public String getRegionName() {
        return regionName;
    }

    public byte[] getRegionNameAsBytes() {
        return regionNameAsBytes;
    }

    public String getRegionServerURL() {
        return regionServerURL;
    }

    public String getHostname() {
        return regionLocation.getHostname();
    }

    public int getTcpPort() {
        return ZooKeeperAdmin.getTcpPort(regionLocation);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof HBaseRegionInfo)) {
            return false;
        }
        return regionName.equals(((HBaseRegionInfo) o).regionName);
    }

    @Override
    public int hashCode() {
        return regionName.hashCode();
    }
}
