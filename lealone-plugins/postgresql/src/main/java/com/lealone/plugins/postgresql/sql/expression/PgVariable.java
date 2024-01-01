/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.postgresql.sql.expression;

import com.lealone.common.util.StringUtils;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueString;
import com.lealone.sql.expression.Variable;

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
