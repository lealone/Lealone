/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
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
        case TRANSACTION_ISOLATION_LEVEL:
            if (stringValue != null)
                session.setTransactionIsolationLevel(stringValue);
            else
                session.setTransactionIsolationLevel(getIntValue());
            // 直接提交事务，开启新事务时用新的隔离级别
            session.commit();
            break;
        case VALUE_VECTOR_FACTORY_NAME:
            session.setValueVectorFactoryName(getStringValue());
            break;
        case EXPRESSION_COMPILE_THRESHOLD:
            session.setExpressionCompileThreshold(getIntValue());
            break;
        case OLAP_OPERATOR_FACTORY_NAME:
            session.setOlapOperatorFactoryName(getStringValue());
            break;
        case OLAP_THRESHOLD:
            session.setOlapThreshold(getIntValue());
            break;
        case OLAP_BATCH_SIZE:
            session.setOlapBatchSize(getIntValue());
            break;
        default:
            DbException.throwInternalError("unknown setting type: " + setting);
        }
        databaseChanged(database);
        return 0;
    }
}
