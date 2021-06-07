/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.runmode;

import org.junit.Test;

public class ClientServerToClientServerTest extends RunModeTest {

    public ClientServerToClientServerTest() {
    }

    @Test
    public void run() throws Exception {
        String dbName = ClientServerToClientServerTest.class.getSimpleName();
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName + " RUN MODE client_server");
        executeUpdate("ALTER DATABASE " + dbName + " RUN MODE client_server PARAMETERS (QUERY_CACHE_SIZE=20)");
        executeUpdate("ALTER DATABASE " + dbName + " PARAMETERS (OPTIMIZE_OR=false)");
    }
}
