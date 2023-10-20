/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mysql.sql.expression;

import java.sql.Connection;
import java.util.TimeZone;

import org.lealone.db.Constants;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueString;
import org.lealone.sql.expression.Variable;

public class MySQLVariable extends Variable {

    public MySQLVariable(ServerSession session, String name) {
        super(session, name);
    }

    @Override
    public Value getValue(ServerSession session) {
        switch (getName().toLowerCase()) {
        case "max_allowed_packet":
            return getInt(4194304);
        case "net_write_timeout":
            return getInt(60);
        case "net_buffer_length":
            return getInt(-1);
        case "auto_increment_increment":
            return getInt(1);
        case "tx_isolation":
        case "transaction_isolation":
            return getString(getTransactionIsolationLevel(session));
        case "time_zone":
        case "system_time_zone":
            return getString(TimeZone.getDefault().getID());
        case "warning_count":
            return getInt(0);
        case "license":
            return getString("SSPL");
        case "version_comment":
            return getString("Lealone-" + Constants.RELEASE_VERSION + " Community Server - SSPL");
        case "character_set_client":
        case "character_set_connection":
        case "character_set_results":
        case "character_set_server":
            return getString("utf-8");
        case "query_cache_size":
            return getInt(1048576);
        case "query_cache_type":
            return getString("ON");
        case "wait_timeout":
            return getInt(28800);
        case "performance_schema":
            return getInt(1);
        case "sql_mode":
            return getString("STRICT_TRANS_TABLES");
        case "lower_case_table_names":
            return getInt(1);
        case "init_connect":
            return getString("");
        case "interactive_timeout":
            return getInt(28800);
        }
        return super.getValue(session);
    }

    private static ValueString getString(String v) {
        return ValueString.get(v);
    }

    private static ValueInt getInt(int v) {
        return ValueInt.get(v);
    }

    public String getTransactionIsolationLevel(ServerSession session) {
        switch (session.getTransactionIsolationLevel()) {
        case Connection.TRANSACTION_READ_COMMITTED:
            return "READ-COMMITTED";
        case Connection.TRANSACTION_REPEATABLE_READ:
            return "REPEATABLE-READ";
        case Connection.TRANSACTION_SERIALIZABLE:
            return "SERIALIZABLE";
        case Connection.TRANSACTION_READ_UNCOMMITTED:
            return "READ-UNCOMMITTED";
        default:
            return "READ-COMMITTED";
        }
    }
}
