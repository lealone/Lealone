/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression;

import java.util.TreeSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.MathUtils;
import org.lealone.db.Mode;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueString;
import org.lealone.sql.expression.evaluator.HotSpotEvaluator;
import org.lealone.sql.expression.function.Function;
import org.lealone.sql.expression.visitor.IExpressionVisitor;
import org.lealone.sql.vector.ValueVector;

/**
 * A mathematical expression, or string concatenation.
 */
public class Operation extends Expression {

    /**
     * This operation represents a string concatenation as in 'Hello' || 'World'.
     */
    public static final int CONCAT = 0;

    /**
     * This operation represents an addition as in 1 + 2.
     */
    public static final int PLUS = 1;

    /**
     * This operation represents a subtraction as in 2 - 1.
     */
    public static final int MINUS = 2;

    /**
     * This operation represents a multiplication as in 2 * 3.
     */
    public static final int MULTIPLY = 3;

    /**
     * This operation represents a division as in 4 * 2.
     */
    public static final int DIVIDE = 4;

    /**
     * This operation represents a negation as in - ID.
     */
    public static final int NEGATE = 5;

    /**
     * This operation represents a modulus as in 5 % 2.
     */
    public static final int MODULUS = 6;

    private int opType;
    private Expression left, right;
    private int dataType;
    private boolean convertRight = true;

    public Operation(int opType, Expression left, Expression right) {
        this.opType = opType;
        this.left = left;
        this.right = right;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    @Override
    public String getSQL(boolean isDistributed) {
        String sql;
        if (opType == NEGATE) {
            // don't remove the space, otherwise it might end up some thing like
            // --1 which is a line remark
            sql = "- " + left.getSQL(isDistributed);
        } else {
            // don't remove the space, otherwise it might end up some thing like
            // --1 which is a line remark
            sql = left.getSQL(isDistributed) + " " + getOperationToken() + " " + right.getSQL(isDistributed);
        }
        return "(" + sql + ")";
    }

    private String getOperationToken() {
        switch (opType) {
        case NEGATE:
            return "-";
        case CONCAT:
            return "||";
        case PLUS:
            return "+";
        case MINUS:
            return "-";
        case MULTIPLY:
            return "*";
        case DIVIDE:
            return "/";
        case MODULUS:
            return "%";
        default:
            throw DbException.getInternalError("opType=" + opType);
        }
    }

    @Override
    public Value getValue(ServerSession session) {
        Value l = left.getValue(session).convertTo(dataType);
        Value r;
        if (right == null) {
            r = null;
        } else {
            r = right.getValue(session);
            if (convertRight) {
                r = r.convertTo(dataType);
            }
        }
        switch (opType) {
        case NEGATE:
            return l == ValueNull.INSTANCE ? l : l.negate();
        case CONCAT: {
            Mode mode = session.getDatabase().getMode();
            return concat(l, r, mode.nullConcatIsNull);
        }
        case PLUS:
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            return l.add(r);
        case MINUS:
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            return l.subtract(r);
        case MULTIPLY:
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            return l.multiply(r);
        case DIVIDE:
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            return l.divide(r);
        case MODULUS:
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            return l.modulus(r);
        default:
            throw DbException.getInternalError("type=" + opType);
        }
    }

    @Override
    public ValueVector getValueVector(ServerSession session) {
        ValueVector l = left.getValueVector(session);
        ValueVector r;
        if (right == null) {
            r = null;
        } else {
            r = right.getValueVector(session);
            if (convertRight) {
                r = r.convertTo(dataType);
            }
        }
        switch (opType) {
        case NEGATE:
            return l.negate();
        case CONCAT: {
            Mode mode = session.getDatabase().getMode();
            return l.concat(r, mode.nullConcatIsNull);
        }
        case PLUS:
            return l.add(r);
        case MINUS:
            return l.subtract(r);
        case MULTIPLY:
            return l.multiply(r);
        case DIVIDE:
            return l.divide(r);
        case MODULUS:
            return l.modulus(r);
        default:
            throw DbException.getInternalError("type=" + opType);
        }
    }

    @Override
    public Expression optimize(ServerSession session) {
        left = left.optimize(session);
        switch (opType) {
        case NEGATE:
            dataType = left.getType();
            if (dataType == Value.UNKNOWN) {
                dataType = Value.DECIMAL;
            }
            break;
        case CONCAT:
            right = right.optimize(session);
            dataType = Value.STRING;
            if (left.isConstant() && right.isConstant()) {
                return ValueExpression.get(getValue(session));
            }
            break;
        case PLUS:
        case MINUS:
        case MULTIPLY:
        case DIVIDE:
        case MODULUS:
            dataType = Value.DECIMAL; // 我加上的
            right = right.optimize(session);
            int l = left.getType();
            int r = right.getType();
            if ((l == Value.NULL && r == Value.NULL) || (l == Value.UNKNOWN && r == Value.UNKNOWN)) {
                // (? + ?) - use decimal by default (the most safe data type) or
                // string when text concatenation with + is enabled
                if (opType == PLUS && session.getDatabase().getMode().allowPlusForStringConcat) {
                    dataType = Value.STRING;
                    opType = CONCAT;
                } else {
                    dataType = Value.DECIMAL;
                }
            } else if (l == Value.DATE || l == Value.TIMESTAMP || l == Value.TIME || r == Value.DATE
                    || r == Value.TIMESTAMP || r == Value.TIME) {
                if (opType == PLUS) {
                    if (r != Value.getHigherOrder(l, r)) {
                        // order left and right: INT < TIME < DATE < TIMESTAMP
                        swap();
                        int t = l;
                        l = r;
                        r = t;
                    }
                    if (l == Value.INT) {
                        // Oracle date add
                        Function f = Function.getFunction(session.getDatabase(), "DATEADD");
                        f.setParameter(0, ValueExpression.get(ValueString.get("DAY")));
                        f.setParameter(1, left);
                        f.setParameter(2, right);
                        f.doneWithParameters();
                        return f.optimize(session);
                    } else if (l == Value.DECIMAL || l == Value.FLOAT || l == Value.DOUBLE) {
                        // Oracle date add
                        Function f = Function.getFunction(session.getDatabase(), "DATEADD");
                        f.setParameter(0, ValueExpression.get(ValueString.get("SECOND")));
                        left = new Operation(Operation.MULTIPLY, ValueExpression.get(ValueInt.get(60 * 60 * 24)), left);
                        f.setParameter(1, left);
                        f.setParameter(2, right);
                        f.doneWithParameters();
                        return f.optimize(session);
                    } else if (l == Value.TIME && r == Value.TIME) {
                        dataType = Value.TIME;
                        return this;
                    } else if (l == Value.TIME) {
                        dataType = Value.TIMESTAMP;
                        return this;
                    }
                } else if (opType == MINUS) {
                    if ((l == Value.DATE || l == Value.TIMESTAMP) && r == Value.INT) {
                        // Oracle date subtract
                        Function f = Function.getFunction(session.getDatabase(), "DATEADD");
                        f.setParameter(0, ValueExpression.get(ValueString.get("DAY")));
                        right = new Operation(NEGATE, right, null);
                        right = right.optimize(session);
                        f.setParameter(1, right);
                        f.setParameter(2, left);
                        f.doneWithParameters();
                        return f.optimize(session);
                    } else if ((l == Value.DATE || l == Value.TIMESTAMP)
                            && (r == Value.DECIMAL || r == Value.FLOAT || r == Value.DOUBLE)) {
                        // Oracle date subtract
                        Function f = Function.getFunction(session.getDatabase(), "DATEADD");
                        f.setParameter(0, ValueExpression.get(ValueString.get("SECOND")));
                        right = new Operation(Operation.MULTIPLY, ValueExpression.get(ValueInt.get(60 * 60 * 24)),
                                right);
                        right = new Operation(NEGATE, right, null);
                        right = right.optimize(session);
                        f.setParameter(1, right);
                        f.setParameter(2, left);
                        f.doneWithParameters();
                        return f.optimize(session);
                    } else if (l == Value.DATE || l == Value.TIMESTAMP) {
                        if (r == Value.TIME) {
                            dataType = Value.TIMESTAMP;
                            return this;
                        } else if (r == Value.DATE || r == Value.TIMESTAMP) {
                            // Oracle date subtract
                            Function f = Function.getFunction(session.getDatabase(), "DATEDIFF");
                            f.setParameter(0, ValueExpression.get(ValueString.get("DAY")));
                            f.setParameter(1, right);
                            f.setParameter(2, left);
                            f.doneWithParameters();
                            return f.optimize(session);
                        }
                    } else if (l == Value.TIME && r == Value.TIME) {
                        dataType = Value.TIME;
                        return this;
                    }
                } else if (opType == MULTIPLY) {
                    if (l == Value.TIME) {
                        dataType = Value.TIME;
                        convertRight = false;
                        return this;
                    } else if (r == Value.TIME) {
                        swap();
                        dataType = Value.TIME;
                        convertRight = false;
                        return this;
                    }
                } else if (opType == DIVIDE) {
                    if (l == Value.TIME) {
                        dataType = Value.TIME;
                        convertRight = false;
                        return this;
                    }
                }
                throw DbException.getUnsupportedException(
                        DataType.getDataType(l).name + " " + getOperationToken() + " " + DataType.getDataType(r).name);
            } else {
                dataType = Value.getHigherOrder(l, r);
                if (DataType.isStringType(dataType) && session.getDatabase().getMode().allowPlusForStringConcat) {
                    opType = CONCAT;
                }
            }
            dataType = Value.DECIMAL; // 我加上的
            break;
        default:
            DbException.throwInternalError("type=" + opType);
        }
        if (left.isConstant() && (right == null || right.isConstant())) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    private void swap() {
        Expression temp = left;
        left = right;
        right = temp;
    }

    @Override
    public int getType() {
        return dataType;
    }

    @Override
    public long getPrecision() {
        if (right != null) {
            switch (opType) {
            case CONCAT:
                return left.getPrecision() + right.getPrecision();
            default:
                return Math.max(left.getPrecision(), right.getPrecision());
            }
        }
        return left.getPrecision();
    }

    @Override
    public int getDisplaySize() {
        if (right != null) {
            switch (opType) {
            case CONCAT:
                return MathUtils.convertLongToInt((long) left.getDisplaySize() + (long) right.getDisplaySize());
            default:
                return Math.max(left.getDisplaySize(), right.getDisplaySize());
            }
        }
        return left.getDisplaySize();
    }

    @Override
    public int getScale() {
        if (right != null) {
            return Math.max(left.getScale(), right.getScale());
        }
        return left.getScale();
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && (right == null || right.isEverything(visitor));
    }

    @Override
    public int getCost() {
        return left.getCost() + 1 + (right == null ? 0 : right.getCost());
    }

    @Override
    public void genCode(HotSpotEvaluator evaluator, StringBuilder buff, TreeSet<String> importSet, int level,
            String retVar) {
        StringBuilder indent = indent((level + 1) * 4);

        buff.append(indent).append("{\r\n");
        String retVarLeft = "lret" + (level + 1);
        String retVarRight = "rret" + (level + 1);
        buff.append(indent).append("    Value ").append(retVarLeft).append(";\r\n");
        if (right != null)
            buff.append(indent).append("    Value ").append(retVarRight).append(";\r\n");
        left.genCode(evaluator, buff, importSet, level + 1, retVarLeft);
        if (right != null)
            right.genCode(evaluator, buff, importSet, level + 1, retVarRight);
        int ltype = left.getType();
        if (ltype != dataType)
            buff.append("    ").append(indent).append(retVarLeft).append(" = ").append(retVarLeft).append(".convertTo(")
                    .append(dataType).append(");\r\n");
        if (right != null) {
            int rtype = right.getType();
            if (convertRight && rtype != dataType)
                buff.append("    ").append(indent).append(retVarRight).append(" = ").append(retVarRight)
                        .append(".convertTo(").append(dataType).append(");\r\n");
        }

        buff.append("    ").append(indent).append(retVar).append(" = ");

        String opTypeName = "";
        switch (opType) {
        case PLUS:
            opTypeName = "add";
            break;
        case MINUS:
            opTypeName = "subtract";
            break;
        case MULTIPLY:
            opTypeName = "multiply";
            break;
        case DIVIDE:
            opTypeName = "divide";
            break;
        case MODULUS:
            opTypeName = "modulus";
            break;
        }

        switch (opType) {
        case NEGATE:
            importSet.add(ValueNull.class.getName());
            buff.append(retVarLeft).append(" == ValueNull.INSTANCE ? ").append(retVarLeft).append(" : ")
                    .append(retVarLeft).append(".negate()").append(";\r\n");
            break;
        case CONCAT:
            importSet.add(Operation.class.getName());
            Mode mode = evaluator.getSession().getDatabase().getMode();
            buff.append("Operation.concat(").append(retVarLeft).append(", ").append(retVarRight).append(", ")
                    .append(mode.nullConcatIsNull).append(");\r\n");
            break;
        default:
            importSet.add(ValueNull.class.getName());
            buff.append(retVarLeft).append(" == ValueNull.INSTANCE || ").append(retVarRight)
                    .append(" == ValueNull.INSTANCE ? ValueNull.INSTANCE : ").append(retVarLeft).append(".")
                    .append(opTypeName).append("(").append(retVarRight).append(")").append(";\r\n");
        }
        buff.append(indent).append("}").append("\r\n");
    }

    public static Value concat(Value l, Value r, boolean nullConcatIsNull) {
        if (l == ValueNull.INSTANCE) {
            if (nullConcatIsNull) {
                return ValueNull.INSTANCE;
            }
            return r;
        } else if (r == ValueNull.INSTANCE) {
            if (nullConcatIsNull) {
                return ValueNull.INSTANCE;
            }
            return l;
        }
        String s1 = l.getString(), s2 = r.getString();
        StringBuilder buff = new StringBuilder(s1.length() + s2.length());
        buff.append(s1).append(s2);
        return ValueString.get(buff.toString());
    }

    @Override
    public <R> R accept(IExpressionVisitor<R> visitor) {
        return visitor.visitOperation(this);
    }
}
