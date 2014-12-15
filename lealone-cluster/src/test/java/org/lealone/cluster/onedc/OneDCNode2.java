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
package org.lealone.cluster.onedc;

import org.lealone.cluster.standalone.LealoneStandalone;

public class OneDCNode2 extends LealoneStandalone {
    public static void main(String[] args) {
        setConfigLoader(OneDCNode2.class);
        System.setProperty("mx4jaddress", "127.0.0.2");
        System.setProperty("mx4jport", "8082");
        run(args, "lealone-onedc.yaml");
    }

    public OneDCNode2() {
        this.listen_address = "127.0.0.2";
        this.dir = "onedc/node2";
    }
}
