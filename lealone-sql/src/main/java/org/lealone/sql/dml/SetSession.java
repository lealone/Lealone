/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.SessionSetting;
import org.lealone.sql.expression.Expression;

/**
 * This class represents the statement
 * SET
 * 
 * @author H2 Group
 * @author zhh
 */
// 只处理session级的类型
public class SetSession extends SetStatement {

    private final SessionSetting setting;

    public SetSession(ServerSession session, SessionSetting type) {
        super(session);
        this.setting = type;
    }

    @Override
    protected String getSettingName() {
        return setting.getName();
    }

    @Override
    public int update() {
        Database database = session.getDatabase();
        switch (setting) {
        case LOCK_TIMEOUT:
            session.setLockTimeout(getAndValidateIntValue());
            break;
        case QUERY_TIMEOUT:
            session.setQueryTimeout(getAndValidateIntValue());
            break;
        case SCHEMA:
            Schema schema = database.getSchema(session, stringValue);
            session.setCurrentSchema(schema);
            break;
        case SCHEMA_SEARCH_PATH:
            session.setSchemaSearchPath(stringValueList);
            break;
        case VARIABLE:
            Expression expr = expression.optimize(session);
            session.setVariable(stringValue, expr.getValue(session));
            break;
        case THROTTLE:
            session.setThrottle(getAndValidateIntValue());
            break;
        default:
            DbException.throwInternalError("unknown setting type: " + setting);
        }
        databaseChanged(database);
        return 0;
    }
}
