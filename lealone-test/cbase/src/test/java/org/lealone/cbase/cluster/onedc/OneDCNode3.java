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
package org.lealone.cbase.cluster.onedc;

import org.lealone.cbase.cluster.NodeBase;

public class OneDCNode3 extends NodeBase {
    public static void main(String[] args) {
        setConfigLoader(OneDCNode3.class);
        System.setProperty("mx4jaddress", "127.0.0.3");
        System.setProperty("mx4jport", "8083");
        run(args, "lealone-onedc.yaml");
    }

    public OneDCNode3() {
        this.listen_address = "127.0.0.3";
        this.dir = "onedc/node3";
    }
}
