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
package org.lealone.cluster.twodc;

import org.lealone.cluster.locator.SnitchProperties;
import org.lealone.cluster.standalone.LealoneStandalone;

public class TwoDCNode1 extends LealoneStandalone {
    public static void main(String[] args) {
        System.setProperty(SnitchProperties.RACKDC_PROPERTY_FILENAME, "lealone-rackdc1.properties");
        setConfigLoader(TwoDCNode1.class);
        run(args, "lealone-twodc.yaml");
    }

    public TwoDCNode1() {
        this.listen_address = "127.0.0.1";
        this.dir = "twodc/node1";
    }
}
