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
package org.lealone.test.start.client_server_mode;

import java.sql.SQLException;

import org.lealone.bootstrap.Lealone;
import org.lealone.test.TestBase;

public class TcpServerStart {
    public static void main(String[] args) throws SQLException {
        setProperty();
        Lealone.main(args);
    }

    private static void setProperty() {
        System.setProperty("lealone.config", "lealone-cs.yaml");

        // System.setProperty("DATABASE_TO_UPPER", "false");
        System.setProperty("lealone.lobInDatabase", "false");
        System.setProperty("lealone.lobClientMaxSizeMemory", "1024");
        System.setProperty("java.io.tmpdir", TestBase.TEST_DIR + "/tmp");
        // System.setProperty("lealone.check2", "true");
    }
}
