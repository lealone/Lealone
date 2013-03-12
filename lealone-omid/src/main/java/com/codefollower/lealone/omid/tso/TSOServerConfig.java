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

package com.codefollower.lealone.omid.tso;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * Holds the configuration parameters of a TSO server instance. 
 *
 */
public class TSOServerConfig {

    static public TSOServerConfig configFactory() {
        return new TSOServerConfig();
    }

    static public TSOServerConfig configFactory(int port, int batch, boolean recoveryEnabled, int ensSize, int qSize,
            String zkservers) {
        return new TSOServerConfig(port, batch, recoveryEnabled, ensSize, qSize, zkservers);
    }

    static public TSOServerConfig parseConfig(String args[]) {
        TSOServerConfig config = new TSOServerConfig();

        if (args.length == 0) {
            new JCommander(config).usage();
            System.exit(0);
        }

        new JCommander(config, args);

        return config;
    }

    @Parameter(names = "-port", description = "Port reserved by the Status Oracle", required = true)
    private int port;

    @Parameter(names = "-batch", description = "Threshold for the batch sent to the WAL")
    private int batch;

    @Parameter(names = "-ha", description = "Highly Available status oracle: "
            + "logs operations to the WAL and recovers from a crash")
    private boolean recoveryEnabled;

    @Parameter(names = "-zk", description = "ZooKeeper ensemble: host1:port1,host2:port2...")
    private String zkServers;

    @Parameter(names = "-ensemble", description = "WAL ensemble size")
    private int ensemble;

    @Parameter(names = "-quorum", description = "WAL quorum size")
    private int quorum;

    TSOServerConfig() {
        this.port = Integer.parseInt(System.getProperty("PORT", "1234"));
        this.batch = Integer.parseInt(System.getProperty("BATCH", "0"));
        this.recoveryEnabled = Boolean.parseBoolean(System.getProperty("RECOVERABLE", "false"));
        this.zkServers = System.getProperty("ZKSERVERS");
        this.ensemble = Integer.parseInt(System.getProperty("ENSEMBLE", "3"));
        this.quorum = Integer.parseInt(System.getProperty("QUORUM", "2"));
    }

    TSOServerConfig(int port, int batch, boolean recoveryEnabled, int ensemble, int quorum, String zkServers) {
        this.port = port;
        this.batch = batch;
        this.recoveryEnabled = recoveryEnabled;
        this.zkServers = zkServers;
        this.ensemble = ensemble;
        this.quorum = quorum;
    }

    public int getPort() {
        return port;
    }

    public int getBatchSize() {
        return batch;
    }

    public boolean isRecoveryEnabled() {
        return recoveryEnabled;
    }

    public String getZkServers() {
        return zkServers;
    }

    public int getEnsembleSize() {
        return ensemble;
    }

    public int getQuorumSize() {
        return quorum;
    }
}
