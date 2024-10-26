/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.constraint;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.StringUtils;
import com.lealone.db.DbObject;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.index.Index;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.result.Result;
import com.lealone.db.row.Row;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.Table;
import com.lealone.db.value.Value;
import com.lealone.sql.IExpression;

/**
 * A check constraint.
 */
public class ConstraintCheck extends Constraint {

    private IExpression.Evaluator exprEvaluator;
    private IExpression expr;

    public ConstraintCheck(Schema schema, int id, String name, Table table) {
        super(schema, id, name, table);
    }

    @Override
    public String getConstraintType() {
        return Constraint.CHECK;
    }

    public void setExpressionEvaluator(IExpression.Evaluator exprEvaluator) {
        this.exprEvaluator = exprEvaluator;
    }

    public void setExpression(IExpression expr) {
        this.expr = expr;
    }

    private String getShortDescription() {
        return getName() + ": " + expr.getSQL();
    }

    @Override
    public String getCreateSQLWithoutIndexes() {
        return getCreateSQL();
    }

    @Override
    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder("ALTER TABLE ");
        buff.append(table.getSQL()).append(" ADD CONSTRAINT ");
        if (table.isHidden()) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append(" CHECK").append(StringUtils.enclose(expr.getSQL())).append(" NOCHECK");
        return buff.toString();
    }

    @Override
    public void removeChildrenAndResources(ServerSession session, DbObjectLock lock) {
        table.removeConstraint(this);
    }

    @Override
    public void checkRow(ServerSession session, Table t, Row oldRow, Row newRow) {
        if (newRow == null) {
            return;
        }
        Value v;
        try {
            synchronized (this) {
                // 这里要同步，多个事务会把exprEvaluator的值修改
                v = exprEvaluator.getExpressionValue(session, expr, newRow);
            }
        } catch (DbException ex) {
            throw DbException.get(ErrorCode.CHECK_CONSTRAINT_INVALID, ex, getShortDescription());
        }
        // Both TRUE and NULL are ok
        if (v.isFalse()) {
            throw DbException.get(ErrorCode.CHECK_CONSTRAINT_VIOLATED_1, getShortDescription());
        }
    }

    @Override
    public boolean usesIndex(Index index) {
        return false;
    }

    @Override
    public void setIndexOwner(Index index) {
        DbException.throwInternalError();
    }

    @Override
    public HashSet<Column> getReferencedColumns(Table table) {
        HashSet<Column> columns = new HashSet<>();
        expr.getColumns(columns);
        for (Iterator<Column> it = columns.iterator(); it.hasNext();) {
            if (it.next().getTable() != table) {
                it.remove();
            }
        }
        return columns;
    }

    public IExpression getExpression() {
        return expr;
    }

    @Override
    public boolean isBefore() {
        return true;
    }

    @Override
    public void checkExistingData(ServerSession session) {
        if (session.getDatabase().isStarting()) {
            // don't check at startup
            return;
        }
        String sql = "SELECT 1 FROM " + table.getSQL() + " WHERE NOT(" + expr.getSQL() + ")";
        Result r = session.executeNestedQueryLocal(sql);
        if (r.next()) {
            throw DbException.get(ErrorCode.CHECK_CONSTRAINT_VIOLATED_1, getName());
        }
    }

    @Override
    public Index getUniqueIndex() {
        return null;
    }

    @Override
    public void rebuild() {
        // nothing to do
    }

    @Override
    public void getDependencies(Set<DbObject> dependencies) {
        expr.getDependencies(dependencies);
    }
}
