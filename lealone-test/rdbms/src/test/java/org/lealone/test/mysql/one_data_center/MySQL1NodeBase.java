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
package org.lealone.test.mysql.one_data_center;

import org.lealone.mysql.service.MySQLDaemon;
import org.lealone.test.NodeBase;

public class MySQL1NodeBase extends NodeBase {
    public static void run(Class<?> loader, String[] args) {
        init(loader, "lealone-onedc.yaml");
        MySQLDaemon.main(args);
    }

    public MySQL1NodeBase() {
        nodeBaseDirPrefix = "mysql/onedc-";
    }

}
