/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.sql.expression;

import org.lealone.common.util.StringUtils;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueString;
import org.lealone.sql.expression.Variable;

public class PgVariable extends Variable {

    public PgVariable(ServerSession session, String name) {
        super(session, name);
    }

    @Override
    public Value getValue(ServerSession session) {
        switch (getName().toLowerCase()) {
        case "search_path":
            return ValueString.get(StringUtils.arrayCombine(session.getSchemaSearchPath(), ','));
        }
        return super.getValue(session);
    }
}
