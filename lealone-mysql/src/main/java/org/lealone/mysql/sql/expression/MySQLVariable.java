/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mysql.sql.expression;

import java.sql.Connection;
import java.util.TimeZone;

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
        case "net_buffer_length":
            return ValueInt.get(-1);
        case "auto_increment_increment":
            return ValueInt.get(1);
        case "tx_isolation":
        case "transaction_isolation":
            return ValueString.get(getTransactionIsolationLevel(session));
        case "time_zone":
        case "system_time_zone":
            return ValueString.get(TimeZone.getDefault().getID());
        }
        return super.getValue(session);
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
