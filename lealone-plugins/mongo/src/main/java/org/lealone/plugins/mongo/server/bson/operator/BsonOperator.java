/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.server.bson.operator;

import org.bson.BsonArray;
import org.bson.BsonValue;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.session.ServerSession;
import org.lealone.plugins.mongo.server.bson.BsonBase;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.function.Function;

public abstract class BsonOperator extends BsonBase {

    public static Function getFunction(ServerSession session, String name, Expression... parameters) {
        Function f = Function.getFunction(session.getDatabase(), name);
        for (int i = 0, len = parameters.length; i < len; i++) {
            f.setParameter(i, parameters[i]);
        }
        f.doneWithParameters();
        return f;
    }

    public static Function getFunction(ServerSession session, String name, BsonValue v) {
        if (!v.isArray())
            return getFunction(session, name, toValueExpression(v));
        BsonArray ba = v.asArray();
        Function f = Function.getFunction(session.getDatabase(), name);
        for (int i = 0, size = ba.size(); i < size; i++) {
            f.setParameter(i, toValueExpression(ba.get(i)));
        }
        f.doneWithParameters();
        return f;
    }

    public static DbException getAUE(String operator) {
        return DbException.getUnsupportedException("aggregate operator " + operator);
    }
}
