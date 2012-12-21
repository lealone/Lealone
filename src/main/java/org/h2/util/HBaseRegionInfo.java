package org.h2.util;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;

public class HBaseRegionInfo {
    private final HRegionInfo regionInfo;
    private final String regionName;
    private final String regionServerURL;

    //
    //    public HBaseRegionInfo(String regionServerURL, String regionName) {
    //        this.regionName = regionName;
    //        this.regionServerURL = regionServerURL;
    //    }

    public HBaseRegionInfo(HRegionLocation regionLocation) {
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
}
