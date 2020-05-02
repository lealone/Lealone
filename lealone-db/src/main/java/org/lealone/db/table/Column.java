/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.table;

import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.Set;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Constants;
import org.lealone.db.DbObject;
import org.lealone.db.Mode;
import org.lealone.db.SQLEngineHolder;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.Row;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.Sequence;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.Session;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueDate;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueString;
import org.lealone.db.value.ValueTime;
import org.lealone.db.value.ValueTimestamp;
import org.lealone.db.value.ValueUuid;
import org.lealone.sql.IExpression;

/**
 * This class represents a column in a table.
 */
public class Column {

    /**
     * The name of the rowid pseudo column.
     */
    public static final String ROWID = "_ROWID_";

    /**
     * This column is not nullable.
     */
    public static final int NOT_NULLABLE = ResultSetMetaData.columnNoNulls;

    /**
     * This column is nullable.
     */
    public static final int NULLABLE = ResultSetMetaData.columnNullable;

    /**
     * It is not know whether this column is nullable.
     */
    public static final int NULLABLE_UNKNOWN = ResultSetMetaData.columnNullableUnknown;

    private final int type;
    private long precision;
    private int scale;
    private int displaySize;
    private Table table;
    private String name;
    private int columnId;
    private boolean nullable = true;
    private IExpression defaultExpression;
    private IExpression checkConstraint;
    private String checkConstraintSQL;
    private String originalSQL;
    private boolean autoIncrement;
    private long start;
    private long increment;
    private boolean convertNullToDefault;
    private Sequence sequence;
    private boolean isComputed;
    private IExpression.Evaluator defaultExpressionEvaluator;
    private IExpression.Evaluator checkConstraintEvaluator;
    private int selectivity;
    private String comment;
    private boolean primaryKey;

    public Column(String name, int type) {
        this(name, type, -1, -1, -1);
    }

    public Column(String name, int type, long precision, int scale, int displaySize) {
        this.name = name;
        this.type = type;
        if (precision == -1 && scale == -1 && displaySize == -1) {
            DataType dt = DataType.getDataType(type);
            precision = dt.defaultPrecision;
            scale = dt.defaultScale;
            displaySize = dt.defaultDisplaySize;
        }
        this.precision = precision;
        this.scale = scale;
        this.displaySize = displaySize;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof Column)) {
            return false;
        }
        Column other = (Column) o;
        if (table == null || other.table == null || name == null || other.name == null) {
            return false;
        }

        if (table != other.table) {
            return false;
        }
        return name.equals(other.name);
    }

    @Override
    public int hashCode() {
        if (table == null || name == null) {
            return 0;
        }
        return table.getId() ^ name.hashCode();
    }

    public Column getClone() {
        Column newColumn = new Column(name, type, precision, scale, displaySize);
        newColumn.copy(this);
        return newColumn;
    }

    /**
     * Convert a value to this column's type.
     *
     * @param v the value
     * @return the value
     */
    public Value convert(Value v) {
        try {
            return v.convertTo(type);
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.DATA_CONVERSION_ERROR_1) {
                String target = (table == null ? "" : table.getName() + ": ") + getCreateSQL();
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, v.getSQL() + " (" + target + ")");
            }
            throw e;
        }
    }

    boolean getComputed() {
        return isComputed;
    }

    /**
     * Compute the value of this computed column.
     *
     * @param session the session
     * @param row the row
     * @return the value
     */
    synchronized Value computeValue(ServerSession session, Row row) {
        return defaultExpressionEvaluator.getExpressionValue(session, defaultExpression, row);
    }

    /**
     * Set the default value in the form of a computed expression of other
     * columns.
     *
     * @param expression the computed expression
     */
    public void setComputedExpression(IExpression expression) {
        this.isComputed = true;
        this.defaultExpression = expression;
    }

    /**
     * Set the table and column id.
     *
     * @param table the table
     * @param columnId the column index
     */
    public void setTable(Table table, int columnId) {
        this.table = table;
        this.columnId = columnId;
    }

    public Table getTable() {
        return table;
    }

    /**
     * Set the default expression.
     *
     * @param session the session
     * @param defaultExpression the default expression
     */
    public void setDefaultExpression(ServerSession session, IExpression defaultExpression) {
        // also to test that no column names are used
        if (defaultExpression != null) {
            defaultExpression = defaultExpression.optimize(session);
            if (defaultExpression.isConstant()) {
                defaultExpression = session.getDatabase().getSQLEngine()
                        .createValueExpression((defaultExpression.getValue(session)));
            }
        }
        this.defaultExpression = defaultExpression;
    }

    public int getColumnId() {
        return columnId;
    }

    public String getSQL() {
        return SQLEngineHolder.quoteIdentifier(name);
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public long getPrecision() {
        return precision;
    }

    public void setPrecision(long p) {
        precision = p;
    }

    public int getDisplaySize() {
        return displaySize;
    }

    public int getScale() {
        return scale;
    }

    public void setNullable(boolean b) {
        nullable = b;
    }

    /**
     * Validate the value, convert it if required, and update the sequence value
     * if required. If the value is null, the default value (NULL if no default
     * is set) is returned. Check constraints are validated as well.
     *
     * @param session the session
     * @param value the value or null
     * @return the new or converted value
     */
    public Value validateConvertUpdateSequence(ServerSession session, Value value) {
        // take a local copy of defaultExpression to avoid holding the lock
        // while calling getValue
        final IExpression localDefaultExpression;
        synchronized (this) {
            localDefaultExpression = defaultExpression;
        }
        if (value == null) {
            if (localDefaultExpression == null) {
                value = ValueNull.INSTANCE;
            } else {
                synchronized (this) {
                    value = localDefaultExpression.getValue(session).convertTo(type);
                }
                if (primaryKey) {
                    session.setLastIdentity(value);
                }
            }
        }
        Mode mode = session.getDatabase().getMode();
        if (value == ValueNull.INSTANCE) {
            if (convertNullToDefault && localDefaultExpression != null) {
                value = localDefaultExpression.getValue(session).convertTo(type);
            }
            if (value == ValueNull.INSTANCE && !nullable) {
                if (mode.convertInsertNullToZero) {
                    DataType dt = DataType.getDataType(type);
                    if (dt.decimal) {
                        value = ValueInt.get(0).convertTo(type);
                    } else if (dt.type == Value.TIMESTAMP) {
                        value = ValueTimestamp.get(new Timestamp(session.getTransactionStart()));
                    } else if (dt.type == Value.TIME) {
                        value = ValueTime.fromNanos(0);
                    } else if (dt.type == Value.DATE) {
                        value = ValueDate.get(new Date(session.getTransactionStart()));
                    } else {
                        value = ValueString.get("").convertTo(type);
                    }
                } else {
                    throw DbException.get(ErrorCode.NULL_NOT_ALLOWED, name);
                }
            }
        }
        if (checkConstraint != null) {
            Value v;
            synchronized (this) {
                v = checkConstraintEvaluator.getExpressionValue(session, checkConstraint, value);
            }
            // Both TRUE and NULL are ok
            if (Boolean.FALSE.equals(v.getBoolean())) {
                throw DbException.get(ErrorCode.CHECK_CONSTRAINT_VIOLATED_1, checkConstraint.getSQL());
            }
        }
        value = value.convertScale(mode.convertOnlyToSmallerScale, scale);
        if (precision > 0) {
            if (!value.checkPrecision(precision)) {
                String s = value.getTraceSQL();
                if (s.length() > 127) {
                    s = s.substring(0, 128) + "...";
                }
                throw DbException.get(ErrorCode.VALUE_TOO_LONG_2, getCreateSQL(),
                        s + " (" + value.getPrecision() + ")");
            }
        }
        updateSequenceIfRequired(session, value);
        return value;
    }

    private void updateSequenceIfRequired(ServerSession session, Value value) {
        if (sequence != null) {
            long current = sequence.getCurrentValue();
            long inc = sequence.getIncrement();
            long now = value.getLong();
            boolean update = false;
            if (inc > 0 && now > current) {
                update = true;
            } else if (inc < 0 && now < current) {
                update = true;
            }
            if (update) {
                sequence.modify(now + inc, null, null, null);
                session.setLastIdentity(ValueLong.get(now));
                sequence.flush(session, 0);
            }
        }
    }

    /**
     * Convert the auto-increment flag to a sequence that is linked with this
     * table.
     *
     * @param session the session
     * @param schema the schema where the sequence should be generated
     * @param id the object id
     * @param temporary true if the sequence is temporary and does not need to
     *            be stored
     */
    public void convertAutoIncrementToSequence(ServerSession session, Schema schema, int id, boolean temporary) {
        if (!autoIncrement) {
            DbException.throwInternalError();
        }
        if ("IDENTITY".equals(originalSQL)) {
            originalSQL = "BIGINT";
        } else if ("SERIAL".equals(originalSQL)) {
            originalSQL = "INT";
        }
        String sequenceName;
        while (true) {
            ValueUuid uuid = ValueUuid.getNewRandom();
            String s = uuid.getString();
            s = s.replace('-', '_').toUpperCase();
            sequenceName = "SYSTEM_SEQUENCE_" + s;
            if (schema.findSequence(session, sequenceName) == null) {
                break;
            }
        }
        Sequence seq = new Sequence(schema, id, sequenceName, start, increment);
        if (temporary) {
            seq.setTemporary(true);
        } else {
            schema.add(session, seq);
        }
        setAutoIncrement(false, 0, 0);
        setDefaultExpression(session, session.getDatabase().getSQLEngine().createSequenceValue(seq));
        setSequence(seq);
    }

    /**
     * Prepare all expressions of this column.
     *
     * @param session the session
     */
    public void prepareExpression(Session session, IExpression.Evaluator evaluator) {
        if (defaultExpression != null) {
            defaultExpression = evaluator.optimizeExpression(session, defaultExpression);
            defaultExpressionEvaluator = evaluator;
        }
    }

    public String getCreateSQL() {
        return getCreateSQL(false);
    }

    public String getCreateSQL(boolean isAlter) {
        StringBuilder buff = new StringBuilder();
        if (name != null) {
            buff.append(SQLEngineHolder.quoteIdentifier(name)).append(' ');
        }
        if (originalSQL != null) {
            buff.append(originalSQL);
        } else {
            buff.append(DataType.getDataType(type).name);
            switch (type) {
            case Value.DECIMAL:
                buff.append('(').append(precision).append(", ").append(scale).append(')');
                break;
            case Value.BYTES:
            case Value.STRING:
            case Value.STRING_IGNORECASE:
            case Value.STRING_FIXED:
                if (precision < Integer.MAX_VALUE) {
                    buff.append('(').append(precision).append(')');
                }
                break;
            default:
            }
        }
        if (defaultExpression != null) {
            String sql = defaultExpression.getSQL();
            if (sql != null) {
                if (isComputed) {
                    buff.append(" AS ").append(sql);
                } else if (defaultExpression != null) {
                    buff.append(" DEFAULT ").append(sql);
                }
            }
        }
        if (!nullable) {
            buff.append(" NOT NULL");
        }
        if (convertNullToDefault) {
            buff.append(" NULL_TO_DEFAULT");
        }
        if (sequence != null) {
            buff.append(" SEQUENCE ").append(sequence.getSQL());
        }
        if (selectivity != 0) {
            buff.append(" SELECTIVITY ").append(selectivity);
        }
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        if (checkConstraint != null) {
            buff.append(" CHECK ").append(checkConstraintSQL);
        }
        return buff.toString();
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setOriginalSQL(String original) {
        originalSQL = original;
    }

    public String getOriginalSQL() {
        return originalSQL;
    }

    public IExpression getDefaultExpression() {
        return defaultExpression;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    /**
     * Set the autoincrement flag and related properties of this column.
     *
     * @param autoInc the new autoincrement flag
     * @param start the sequence start value
     * @param increment the sequence increment
     */
    public void setAutoIncrement(boolean autoInc, long start, long increment) {
        this.autoIncrement = autoInc;
        this.start = start;
        this.increment = increment;
        this.nullable = false;
        if (autoInc) {
            convertNullToDefault = true;
        }
    }

    public void setConvertNullToDefault(boolean convert) {
        this.convertNullToDefault = convert;
    }

    /**
     * Rename the column. This method will only set the column name to the new
     * value.
     *
     * @param newName the new column name
     */
    public void rename(String newName) {
        this.name = newName;
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }

    public Sequence getSequence() {
        return sequence;
    }

    /**
     * Get the selectivity of the column. Selectivity 100 means values are
     * unique, 10 means every distinct value appears 10 times on average.
     *
     * @return the selectivity
     */
    public int getSelectivity() {
        return selectivity == 0 ? Constants.SELECTIVITY_DEFAULT : selectivity;
    }

    /**
     * Set the new selectivity of a column.
     *
     * @param selectivity the new value
     */
    public void setSelectivity(int selectivity) {
        selectivity = selectivity < 0 ? 0 : (selectivity > 100 ? 100 : selectivity);
        this.selectivity = selectivity;
    }

    /**
     * Add a check constraint expression to this column. An existing check
     * constraint constraint is added using AND.
     *
     * @param session the session
     * @param expr the (additional) constraint
     */
    public void addCheckConstraint(ServerSession session, IExpression expr, IExpression.Evaluator evaluator) {
        if (expr == null) {
            return;
        }
        synchronized (this) {
            String oldName = name;
            if (name == null) {
                name = "VALUE";
            }
            expr = evaluator.optimizeExpression(session, expr);
            name = oldName;
        }
        // check if the column is mapped
        synchronized (this) {
            evaluator.getExpressionValue(session, expr, ValueNull.INSTANCE);
        }
        if (checkConstraint == null) {
            checkConstraint = expr;
            checkConstraintEvaluator = evaluator;
        } else {
            checkConstraint = session.getDatabase().getSQLEngine().createConditionAndOr(true, checkConstraint, expr);
        }
        checkConstraintSQL = getCheckConstraintSQL(session, name);

    }

    /**
     * Remove the check constraint if there is one.
     */
    public void removeCheckConstraint() {
        checkConstraint = null;
        checkConstraintSQL = null;
    }

    /**
     * Get the check constraint expression for this column if set.
     *
     * @param session the session
     * @param asColumnName the column name to use
     * @return the constraint expression
     */
    public IExpression getCheckConstraint(ServerSession session, String asColumnName) {
        if (checkConstraint == null) {
            return null;
        }
        org.lealone.sql.SQLParser parser = session.getDatabase().createParser(session);
        String sql;
        synchronized (this) {
            String oldName = name;
            name = asColumnName;
            sql = checkConstraint.getSQL();
            name = oldName;
        }
        IExpression expr = parser.parseExpression(sql);
        return expr;
    }

    String getDefaultSQL() {
        return defaultExpression == null ? null : defaultExpression.getSQL();
    }

    int getPrecisionAsInt() {
        return MathUtils.convertLongToInt(precision);
    }

    DataType getDataType() {
        return DataType.getDataType(type);
    }

    /**
     * Get the check constraint SQL snippet.
     *
     * @param session the session
     * @param asColumnName the column name to use
     * @return the SQL snippet
     */
    String getCheckConstraintSQL(ServerSession session, String asColumnName) {
        IExpression constraint = getCheckConstraint(session, asColumnName);
        return constraint == null ? "" : constraint.getSQL();
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    void getDependencies(Set<DbObject> dependencies) {
        if (sequence != null) {
            dependencies.add(sequence);
        }
        if (defaultExpression != null) {
            defaultExpression.getDependencies(dependencies);
        }
        if (checkConstraint != null) {
            checkConstraint.getDependencies(dependencies);
        }
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Check whether the new column is of the same type and not more restricted
     * than this column.
     *
     * @param newColumn the new (target) column
     * @return true if the new column is compatible
     */
    public boolean isWideningConversion(Column newColumn) {
        if (type != newColumn.type) {
            return false;
        }
        if (precision > newColumn.precision) {
            return false;
        }
        if (scale != newColumn.scale) {
            return false;
        }
        if (nullable && !newColumn.nullable) {
            return false;
        }
        if (convertNullToDefault != newColumn.convertNullToDefault) {
            return false;
        }
        if (primaryKey != newColumn.primaryKey) {
            return false;
        }
        if (autoIncrement || newColumn.autoIncrement) {
            return false;
        }
        if (checkConstraint != null || newColumn.checkConstraint != null) {
            return false;
        }
        if (convertNullToDefault || newColumn.convertNullToDefault) {
            return false;
        }
        if (defaultExpression != null || newColumn.defaultExpression != null) {
            return false;
        }
        if (isComputed || newColumn.isComputed) {
            return false;
        }
        return true;
    }

    /**
     * Copy the data of the source column into the current column.
     *
     * @param source the source column
     */
    public void copy(Column source) {
        checkConstraint = source.checkConstraint;
        checkConstraintSQL = source.checkConstraintSQL;
        displaySize = source.displaySize;
        name = source.name;
        precision = source.precision;
        scale = source.scale;
        // table is not set
        // columnId is not set
        nullable = source.nullable;
        defaultExpression = source.defaultExpression;
        defaultExpressionEvaluator = source.defaultExpressionEvaluator;
        originalSQL = source.originalSQL;
        // autoIncrement, start, increment is not set
        convertNullToDefault = source.convertNullToDefault;
        sequence = source.sequence;
        comment = source.comment;
        isComputed = source.isComputed;
        selectivity = source.selectivity;
        primaryKey = source.primaryKey;
    }

}
