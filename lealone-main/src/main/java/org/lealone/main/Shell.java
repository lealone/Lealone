/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.main;

import java.sql.Connection;
import java.sql.SQLException;

import org.lealone.client.LealoneClient;
import org.lealone.db.ConnectionInfo;

public class Shell extends LealoneClient {

    public static void main(String[] args) {
        Shell shell = new Shell(args);
        main(shell);
    }

    public Shell(String[] args) {
        super(args);
    }

    @Override
    protected Connection getConnection() throws SQLException {
        ConnectionInfo ci = getConnectionInfo();
        if (ci.isEmbedded()) {
            Lealone.embed();
        }
        return getConnectionSync(ci);
    }
}
