package org.lealone.plugins.mysql.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;

@SuppressWarnings("unused")
public class CreateProcedure extends CreateRoutine {

    private StatementBase prepared;

    public CreateProcedure(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.PREPARE;
    }

    @Override
    public void setPrepared(StatementBase prep) {
        this.prepared = prep;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.FUNCTION_ALIAS, session);
        if (lock == null)
            return -1;

        if (schema.findFunction(session, name) != null) {
            if (!ifNotExists) {
                throw DbException.get(ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1, name);
            }
        } else {
            // int id = getObjectId();
            // FunctionAlias functionAlias;
            // if (javaClassMethod != null) {
            // functionAlias = FunctionAlias.newInstance(schema, id, aliasName, javaClassMethod, force,
            // bufferResultSetToLocalTemp);
            // } else {
            // functionAlias = FunctionAlias.newInstanceFromSource(schema, id, aliasName, source, force,
            // bufferResultSetToLocalTemp);
            // }
            // functionAlias.setDeterministic(deterministic);
            // schema.add(session, functionAlias, lock);
        }
        return 0;
    }

    public static class Parameter {

    }
}
