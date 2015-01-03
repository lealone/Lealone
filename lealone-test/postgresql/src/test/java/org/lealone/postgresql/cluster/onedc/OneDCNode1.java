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
package org.lealone.postgresql.cluster.onedc;

import org.lealone.postgresql.cluster.NodeBase;

public class OneDCNode1 extends NodeBase {
    public static void main(String[] args) {
        setConfigLoader(OneDCNode1.class);
        run(args, "lealone-onedc.yaml");
    }

    public OneDCNode1() {
        this.listen_address = "127.0.0.1";
        this.dir = "onedc/node1";
        this.url = "jdbc:postgresql://localhost:5431/lealone";
    }
}
