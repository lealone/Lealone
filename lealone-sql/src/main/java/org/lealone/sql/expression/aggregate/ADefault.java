/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.aggregate;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.session.ServerSession;
import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBoolean;
import org.lealone.db.value.ValueDouble;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.query.Select;
import org.lealone.sql.vector.ValueVector;

public class ADefault extends Aggregate {

    public ADefault(int type, Expression on, Select select, boolean distinct) {
        super(type, on, select, distinct);
    }

    @Override
    public Expression optimize(ServerSession session) {
        super.optimize(session);
        switch (type) {
        case SUM:
            if (dataType == Value.BOOLEAN) {
                // example: sum(id > 3) (count the rows)
                dataType = Value.LONG;
            } else if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            } else {
                dataType = DataType.getAddProofType(dataType);
            }
            break;
        case AVG:
            if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            }
            break;
        case MIN:
        case MAX:
            break;
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
            dataType = Value.DOUBLE;
            precision = ValueDouble.PRECISION;
            displaySize = ValueDouble.DISPLAY_SIZE;
            scale = 0;
            break;
        case BOOL_AND:
        case BOOL_OR:
            dataType = Value.BOOLEAN;
            precision = ValueBoolean.PRECISION;
            displaySize = ValueBoolean.DISPLAY_SIZE;
            scale = 0;
            break;
        case BIT_AND:
        case BIT_OR:
            if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            }
            break;
        default:
            DbException.throwInternalError("type=" + type);
        }
        return this;
    }

    @Override
    protected AggregateData createAggregateData() {
        return new AggregateDataDefault(type);
    }

    @Override
    public String getSQL(boolean isDistributed) {
        String text;
        switch (type) {
        case SUM:
            text = "SUM";
            break;
        case MIN:
            text = "MIN";
            break;
        case MAX:
            text = "MAX";
            break;
        case AVG:
            if (isDistributed) {
                if (distinct) {
                    return "COUNT(DISTINCT " + on.getSQL(isDistributed) + "), SUM(DISTINCT " + on.getSQL(isDistributed)
                            + ")";
                } else {
                    return "COUNT(" + on.getSQL(isDistributed) + "), SUM(" + on.getSQL(isDistributed) + ")";
                }
            }
            text = "AVG";
            break;
        case STDDEV_POP:
            if (isDistributed)
                return getSQL_STDDEV_VAR();
            text = "STDDEV_POP";
            break;
        case STDDEV_SAMP:
            if (isDistributed)
                return getSQL_STDDEV_VAR();
            text = "STDDEV_SAMP";
            break;
        case VAR_POP:
            if (isDistributed)
                return getSQL_STDDEV_VAR();
            text = "VAR_POP";
            break;
        case VAR_SAMP:
            if (isDistributed)
                return getSQL_STDDEV_VAR();
            text = "VAR_SAMP";
            break;
        case BOOL_AND:
            text = "BOOL_AND";
            break;
        case BOOL_OR:
            text = "BOOL_OR";
            break;
        case BIT_AND:
            text = "BIT_AND";
            break;
        case BIT_OR:
            text = "BIT_OR";
            break;
        default:
            throw DbException.getInternalError("type=" + type);
        }
        return getSQL(text, isDistributed);
    }

    private String getSQL_STDDEV_VAR() {
        String onSQL = on.getSQL(true);
        if (distinct) {
            return "COUNT(DISTINCT " + onSQL + "), SUM(DISTINCT " + onSQL + "), SUM(DISTINCT " + onSQL + " * " + onSQL
                    + ")";
        } else {
            return "COUNT(" + onSQL + "), SUM(" + onSQL + "), SUM(" + onSQL + " * " + onSQL + ")";
        }
    }

    private class AggregateDataDefault extends AggregateData {

        private final int aggregateType;
        private long count;
        private ValueHashMap<AggregateDataDefault> distinctValues;
        private Value value;
        private double m2, mean;

        private ValueVector vv;
        // private ValueVector bvv;

        /**
         * @param aggregateType the type of the aggregate operation
         */
        AggregateDataDefault(int aggregateType) {
            this.aggregateType = aggregateType;
        }

        @Override
        void add(Database database, Value v) {
            add(database, v, distinct);
        }

        private void add(Database database, Value v, boolean distinct) {
            if (v == ValueNull.INSTANCE) {
                return;
            }
            count++;
            if (distinct) {
                if (distinctValues == null) {
                    distinctValues = ValueHashMap.newInstance();
                }
                distinctValues.put(v, this);
                return;
            }
            switch (aggregateType) {
            case Aggregate.SUM:
                if (value == null) {
                    value = v.convertTo(dataType);
                } else {
                    v = v.convertTo(value.getType());
                    value = value.add(v);
                }
                break;
            case Aggregate.AVG:
                if (value == null) {
                    value = v.convertTo(DataType.getAddProofType(dataType));
                } else {
                    v = v.convertTo(value.getType());
                    value = value.add(v);
                }
                break;
            case Aggregate.MIN:
                if (value == null || database.compare(v, value) < 0) {
                    value = v;
                }
                break;
            case Aggregate.MAX:
                if (value == null || database.compare(v, value) > 0) {
                    value = v;
                }
                break;
            case Aggregate.STDDEV_POP:
            case Aggregate.STDDEV_SAMP:
            case Aggregate.VAR_POP:
            case Aggregate.VAR_SAMP: {
                // Using Welford's method, see also
                // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
                // http://www.johndcook.com/standard_deviation.html
                double x = v.getDouble();
                if (count == 1) {
                    mean = x;
                    m2 = 0;
                } else {
                    double delta = x - mean;
                    mean += delta / count;
                    m2 += delta * (x - mean);
                }
                break;
            }
            case Aggregate.BOOL_AND:
                v = v.convertTo(Value.BOOLEAN);
                if (value == null) {
                    value = v;
                } else {
                    value = ValueBoolean.get(value.getBoolean() && v.getBoolean());
                }
                break;
            case Aggregate.BOOL_OR:
                v = v.convertTo(Value.BOOLEAN);
                if (value == null) {
                    value = v;
                } else {
                    value = ValueBoolean.get(value.getBoolean() || v.getBoolean());
                }
                break;
            case Aggregate.BIT_AND:
                if (value == null) {
                    value = v.convertTo(dataType);
                } else {
                    value = ValueLong.get(value.getLong() & v.getLong()).convertTo(dataType);
                }
                break;
            case Aggregate.BIT_OR:
                if (value == null) {
                    value = v.convertTo(dataType);
                } else {
                    value = ValueLong.get(value.getLong() | v.getLong()).convertTo(dataType);
                }
                break;
            default:
                DbException.throwInternalError("type=" + aggregateType);
            }
        }

        @Override
        void add(Database database, ValueVector bvv, ValueVector vv) {
            Value v = vv.getValue(0);
            if (v == ValueNull.INSTANCE) {
                return;
            }
            count++;
            if (distinct) {
                if (distinctValues == null) {
                    distinctValues = ValueHashMap.newInstance();
                }
                distinctValues.put(v, this);
                return;
            }
            switch (aggregateType) {
            case Aggregate.SUM:
                if (this.vv == null) {
                    // value = v.convertTo(dataType);
                    this.vv = vv;
                    // this.bvv = bvv;
                } else {
                    // v = v.convertTo(value.getType());
                    this.vv = this.vv.add(vv);
                }
                break;
            case Aggregate.AVG:
                if (value == null) {
                    value = v.convertTo(DataType.getAddProofType(dataType));
                } else {
                    v = v.convertTo(value.getType());
                    value = value.add(v);
                }
                break;
            case Aggregate.MIN:
                if (value == null || database.compare(v, value) < 0) {
                    value = v;
                }
                break;
            case Aggregate.MAX:
                if (value == null || database.compare(v, value) > 0) {
                    value = v;
                }
                break;
            case Aggregate.STDDEV_POP:
            case Aggregate.STDDEV_SAMP:
            case Aggregate.VAR_POP:
            case Aggregate.VAR_SAMP: {
                // Using Welford's method, see also
                // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
                // http://www.johndcook.com/standard_deviation.html
                double x = v.getDouble();
                if (count == 1) {
                    mean = x;
                    m2 = 0;
                } else {
                    double delta = x - mean;
                    mean += delta / count;
                    m2 += delta * (x - mean);
                }
                break;
            }
            case Aggregate.BOOL_AND:
                v = v.convertTo(Value.BOOLEAN);
                if (value == null) {
                    value = v;
                } else {
                    value = ValueBoolean.get(value.getBoolean() && v.getBoolean());
                }
                break;
            case Aggregate.BOOL_OR:
                v = v.convertTo(Value.BOOLEAN);
                if (value == null) {
                    value = v;
                } else {
                    value = ValueBoolean.get(value.getBoolean() || v.getBoolean());
                }
                break;
            case Aggregate.BIT_AND:
                if (value == null) {
                    value = v.convertTo(dataType);
                } else {
                    value = ValueLong.get(value.getLong() & v.getLong()).convertTo(dataType);
                }
                break;
            case Aggregate.BIT_OR:
                if (value == null) {
                    value = v.convertTo(dataType);
                } else {
                    value = ValueLong.get(value.getLong() | v.getLong()).convertTo(dataType);
                }
                break;
            default:
                DbException.throwInternalError("type=" + aggregateType);
            }
        }

        @Override
        Value getValue(Database database) {
            if (distinct) {
                count = 0;
                groupDistinct(database, dataType);
            }
            Value v = null;
            switch (aggregateType) {
            case Aggregate.SUM:
            case Aggregate.MIN:
            case Aggregate.MAX:
            case Aggregate.BIT_OR:
            case Aggregate.BIT_AND:
            case Aggregate.BOOL_OR:
            case Aggregate.BOOL_AND:
                if (this.vv != null) {
                    v = vv.sum();
                } else {
                    v = value;
                }
                break;
            case Aggregate.AVG:
                if (value != null) {
                    v = Aggregate.divide(value, count);
                }
                break;
            case Aggregate.STDDEV_POP: {
                if (count < 1) {
                    return ValueNull.INSTANCE;
                }
                v = ValueDouble.get(Math.sqrt(m2 / count));
                break;
            }
            case Aggregate.STDDEV_SAMP: {
                if (count < 2) {
                    return ValueNull.INSTANCE;
                }
                v = ValueDouble.get(Math.sqrt(m2 / (count - 1)));
                break;
            }
            case Aggregate.VAR_POP: {
                if (count < 1) {
                    return ValueNull.INSTANCE;
                }
                v = ValueDouble.get(m2 / count);
                break;
            }
            case Aggregate.VAR_SAMP: {
                if (count < 2) {
                    return ValueNull.INSTANCE;
                }
                v = ValueDouble.get(m2 / (count - 1));
                break;
            }
            default:
                DbException.throwInternalError("type=" + aggregateType);
            }
            return v == null ? ValueNull.INSTANCE : v.convertTo(dataType);
        }

        private void groupDistinct(Database database, int dataType) {
            if (distinctValues == null) {
                return;
            }
            count = 0;
            for (Value v : distinctValues.keys()) {
                add(database, v, false);
            }
        }

        @Override
        void merge(Database database, Value v) {
            if (v == ValueNull.INSTANCE) {
                return;
            }
            count++;
            if (distinct) {
                if (distinctValues == null) {
                    distinctValues = ValueHashMap.newInstance();
                }
                distinctValues.put(v, this);
                return;
            }
            switch (aggregateType) {
            case Aggregate.SUM:
                if (value == null) {
                    value = v.convertTo(dataType);
                } else {
                    v = v.convertTo(value.getType());
                    value = value.add(v);
                }
                break;
            case Aggregate.MIN:
                if (value == null || database.compare(v, value) < 0) {
                    value = v;
                }
                break;
            case Aggregate.MAX:
                if (value == null || database.compare(v, value) > 0) {
                    value = v;
                }
                break;
            case Aggregate.BOOL_AND:
                v = v.convertTo(Value.BOOLEAN);
                if (value == null) {
                    value = v;
                } else {
                    value = ValueBoolean.get(value.getBoolean() && v.getBoolean());
                }
                break;
            case Aggregate.BOOL_OR:
                v = v.convertTo(Value.BOOLEAN);
                if (value == null) {
                    value = v;
                } else {
                    value = ValueBoolean.get(value.getBoolean() || v.getBoolean());
                }
                break;
            case Aggregate.BIT_AND:
                if (value == null) {
                    value = v.convertTo(dataType);
                } else {
                    value = ValueLong.get(value.getLong() & v.getLong()).convertTo(dataType);
                }
                break;
            case Aggregate.BIT_OR:
                if (value == null) {
                    value = v.convertTo(dataType);
                } else {
                    value = ValueLong.get(value.getLong() | v.getLong()).convertTo(dataType);
                }
                break;
            // 这5个在分布式环境下会进行重写，所以合并时是不会出现的
            case Aggregate.AVG:
            case Aggregate.STDDEV_POP:
            case Aggregate.STDDEV_SAMP:
            case Aggregate.VAR_POP:
            case Aggregate.VAR_SAMP:
            default:
                DbException.throwInternalError("type=" + aggregateType);
            }
        }

        @Override
        Value getMergedValue(Database database) {
            if (distinct) {
                count = 0;
                groupDistinct(database, dataType);
            }
            Value v = null;
            switch (aggregateType) {
            case Aggregate.SUM:
            case Aggregate.MIN:
            case Aggregate.MAX:
            case Aggregate.BOOL_AND:
            case Aggregate.BOOL_OR:
            case Aggregate.BIT_AND:
            case Aggregate.BIT_OR:
                v = value;
                break;
            case Aggregate.AVG:
            case Aggregate.STDDEV_POP:
            case Aggregate.STDDEV_SAMP:
            case Aggregate.VAR_POP:
            case Aggregate.VAR_SAMP:
            default:
                DbException.throwInternalError("type=" + aggregateType);
            }
            return v == null ? ValueNull.INSTANCE : v.convertTo(dataType);
        }
    }
}
