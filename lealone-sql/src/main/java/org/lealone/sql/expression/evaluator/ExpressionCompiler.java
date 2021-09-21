/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.evaluator;

import java.util.TreeSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Constants;
import org.lealone.db.Mode;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.ServerSession;
import org.lealone.db.util.SourceCompiler;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBoolean;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.StatementBase;
import org.lealone.sql.expression.Alias;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.Operation;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.expression.Rownum;
import org.lealone.sql.expression.SequenceValue;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.Variable;
import org.lealone.sql.expression.Wildcard;
import org.lealone.sql.expression.aggregate.AGroupConcat;
import org.lealone.sql.expression.aggregate.Aggregate;
import org.lealone.sql.expression.aggregate.JavaAggregate;
import org.lealone.sql.expression.condition.CompareLike;
import org.lealone.sql.expression.condition.Comparison;
import org.lealone.sql.expression.condition.ConditionAndOr;
import org.lealone.sql.expression.condition.ConditionExists;
import org.lealone.sql.expression.condition.ConditionIn;
import org.lealone.sql.expression.condition.ConditionInConstantSet;
import org.lealone.sql.expression.condition.ConditionInSelect;
import org.lealone.sql.expression.condition.ConditionNot;
import org.lealone.sql.expression.function.Function;
import org.lealone.sql.expression.function.JavaFunction;
import org.lealone.sql.expression.function.TableFunction;
import org.lealone.sql.expression.subquery.SubQuery;
import org.lealone.sql.expression.visitor.ExpressionVisitorBase;

public class ExpressionCompiler extends ExpressionVisitorBase<Void> {

    private static long id;

    public static synchronized JitEvaluator createJitEvaluator(HotSpotEvaluator evaluator, ServerSession session,
            Expression expression) {
        StringBuilder body = new StringBuilder();
        TreeSet<String> importSet = new TreeSet<>();
        importSet.add(JitEvaluator.class.getName());
        importSet.add(Value.class.getName());
        expression.accept(new ExpressionCompiler(evaluator, body, importSet, 1, "ret1"));

        id++;
        String className = "JitEvaluator" + id;
        StringBuilder buff = new StringBuilder();
        buff.append("package ").append(Constants.USER_PACKAGE).append(".expression.evaluator").append(";\r\n\r\n");
        for (String p : importSet) {
            buff.append("import ").append(p).append(";\r\n");
        }
        buff.append("\r\n");
        buff.append("public class ").append(className).append(" extends JitEvaluator").append(" {\r\n");
        buff.append("    @Override").append("\r\n");
        buff.append("    public boolean getBooleanValue() {").append("\r\n");
        buff.append("        Value ret1").append(";\r\n");
        buff.append(body);
        buff.append("        return ret1.getBoolean();").append("\r\n");
        buff.append("    }").append("\r\n");
        buff.append("}\r\n");

        String fullClassName = Constants.USER_PACKAGE + ".expression.evaluator." + className;
        return loadFromSource(session, buff, fullClassName);
    }

    private static JitEvaluator loadFromSource(ServerSession session, StringBuilder buff, String fullClassName) {
        // SourceCompiler compiler = session.getDatabase().getCompiler();
        // compiler.setSource(fullClassName, buff.toString());
        // try {
        // return (JitEvaluator) compiler.getClass(fullClassName).getDeclaredConstructor().newInstance();
        // } catch (Exception e) {
        // throw DbException.convert(e);
        // }

        try {
            return (JitEvaluator) SourceCompiler.compileAsClass(fullClassName, buff.toString()).getDeclaredConstructor()
                    .newInstance();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public static synchronized void createJitEvaluatorAsync(HotSpotEvaluator evaluator, ServerSession session,
            Expression expression, AsyncHandler<AsyncResult<JitEvaluator>> asyncHandler) {
        id++;
        Thread t = new Thread(() -> {
            try {
                JitEvaluator e = createJitEvaluator(evaluator, session, expression);
                asyncHandler.handle(new AsyncResult<>(e));
            } catch (Exception e) {
                asyncHandler.handle(new AsyncResult<>(e));
            }
        });
        t.setName("AsyncCreateJitEvaluatorThread-" + id);
        t.start();
    }

    private static StringBuilder indent(int size) {
        StringBuilder indent = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            indent.append(' ');
        }
        return indent;
    }

    private HotSpotEvaluator evaluator;
    private StringBuilder buff;
    private TreeSet<String> importSet;
    private int level;
    private String retVar;

    private ExpressionCompiler(HotSpotEvaluator evaluator, StringBuilder buff, TreeSet<String> importSet, int level,
            String retVar) {
        this.evaluator = evaluator;
        this.buff = buff;
        this.importSet = importSet;
        this.level = level;
        this.retVar = retVar;
    }

    private ExpressionCompiler copy(int level, String retVar) {
        return new ExpressionCompiler(evaluator, buff, importSet, level, retVar);
    }

    @Override
    public Void visitExpression(Expression e) { // 默认回退到解释执行的方式
        StringBuilder indent = indent((level + 1) * 4);
        evaluator.addExpression(e);
        buff.append(indent).append(retVar).append(" = evaluator.getExpression(")
                .append(evaluator.getExpressionListSize() - 1).append(").getValue(session);\r\n");
        return null;
    }

    @Override
    public Void visitExpressionColumn(ExpressionColumn e) {
        StringBuilder indent = indent((level + 1) * 4);
        evaluator.addExpressionColumn(e);
        buff.append(indent).append(retVar).append(" = evaluator.getExpressionColumn(")
                .append(evaluator.getExpressionColumnListSize() - 1).append(").getValue(session);\r\n");
        return null;
    }

    @Override
    public Void visitOperation(Operation e) {
        Expression left = e.getLeft(), right = e.getRight();
        StringBuilder indent = indent((level + 1) * 4);

        buff.append(indent).append("{\r\n");
        String retVarLeft = "lret" + (level + 1);
        String retVarRight = "rret" + (level + 1);
        buff.append(indent).append("    Value ").append(retVarLeft).append(";\r\n");
        if (right != null)
            buff.append(indent).append("    Value ").append(retVarRight).append(";\r\n");
        left.accept(copy(level + 1, retVarLeft));
        if (right != null)
            right.accept(copy(level + 1, retVarRight));
        int ltype = left.getType();
        int dataType = e.getDataType();
        if (ltype != dataType)
            buff.append("    ").append(indent).append(retVarLeft).append(" = ").append(retVarLeft).append(".convertTo(")
                    .append(dataType).append(");\r\n");
        if (right != null) {
            int rtype = right.getType();
            if (e.isConvertRight() && rtype != dataType)
                buff.append("    ").append(indent).append(retVarRight).append(" = ").append(retVarRight)
                        .append(".convertTo(").append(dataType).append(");\r\n");
        }

        buff.append("    ").append(indent).append(retVar).append(" = ");

        String opTypeName = "";
        switch (e.getOpType()) {
        case Operation.PLUS:
            opTypeName = "add";
            break;
        case Operation.MINUS:
            opTypeName = "subtract";
            break;
        case Operation.MULTIPLY:
            opTypeName = "multiply";
            break;
        case Operation.DIVIDE:
            opTypeName = "divide";
            break;
        case Operation.MODULUS:
            opTypeName = "modulus";
            break;
        }

        switch (e.getOpType()) {
        case Operation.NEGATE:
            importSet.add(ValueNull.class.getName());
            buff.append(retVarLeft).append(" == ValueNull.INSTANCE ? ").append(retVarLeft).append(" : ")
                    .append(retVarLeft).append(".negate()").append(";\r\n");
            break;
        case Operation.CONCAT:
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
        return null;
    }

    @Override
    public Void visitParameter(Parameter e) {
        StringBuilder indent = indent((level + 1) * 4);
        Value value = e.getValue();
        if (value == null || value == ValueNull.INSTANCE) {
            importSet.add(ValueNull.class.getName());
            buff.append(indent).append(retVar).append(" = ValueNull.INSTANCE;\r\n");
        } else {
            evaluator.addValue(value);
            buff.append(indent).append(retVar).append(" = evaluator.getValue(").append(evaluator.getValueListSize() - 1)
                    .append(");\r\n");
        }
        return null;
    }

    @Override
    public Void visitRownum(Rownum e) {
        StringBuilder indent = indent((level + 1) * 4);
        importSet.add(ValueInt.class.getName());
        importSet.add(StatementBase.class.getName());
        buff.append(indent).append(retVar)
                .append(" = ValueInt.get(((StatementBase)session.getCurrentCommand()).getCurrentRowNumber());\r\n");
        return null;
    }

    @Override
    public Void visitValueExpression(ValueExpression e) {
        StringBuilder indent = indent((level + 1) * 4);
        evaluator.addValue(e.getValue(null));
        buff.append(indent).append(retVar).append(" = evaluator.getValue(").append(evaluator.getValueListSize() - 1)
                .append(");\r\n");
        return null;
    }

    @Override
    public Void visitVariable(Variable e) {
        StringBuilder indent = indent((level + 1) * 4);
        buff.append(indent).append(retVar).append(" = session.getVariable(\"").append(e.getName()).append("\");\r\n");
        return null;
    }

    @Override
    public Void visitComparison(Comparison e) {
        Expression left = e.getLeft(), right = e.getRight();
        StringBuilder indent = indent((level + 1) * 4);
        importSet.add(Comparison.class.getName());
        importSet.add(ValueBoolean.class.getName());
        buff.append(indent).append("{\r\n");
        String retVarLeft = "lret" + (level + 1);
        String retVarRight = "rret" + (level + 1);
        buff.append(indent).append("    Value ").append(retVarLeft).append(";\r\n");
        buff.append(indent).append("    Value ").append(retVarRight).append(";\r\n");
        left.accept(copy(level + 1, retVarLeft));
        right.accept(copy(level + 1, retVarRight));
        int dataType = Value.getHigherOrder(left.getType(), right.getType());
        int ltype = left.getType();
        if (ltype != dataType)
            buff.append("    ").append(indent).append(retVarLeft).append(" = ").append(retVarLeft).append(".convertTo(")
                    .append(dataType).append(");\r\n");
        int rtype = right.getType();
        if (rtype != dataType)
            buff.append("    ").append(indent).append(retVarRight).append(" = ").append(retVarRight)
                    .append(".convertTo(").append(dataType).append(");\r\n");
        buff.append("    ").append(indent).append("boolean result = Comparison.compareNotNull(session.getDatabase(), ")
                .append(retVarLeft).append(", ").append(retVarRight).append(", ").append(e.getCompareType())
                .append(");\r\n");
        buff.append("    ").append(indent).append(retVar).append(" = ValueBoolean.get(result);\r\n");
        buff.append(indent).append("}").append("\r\n");
        return null;
    }

    @Override
    public Void visitConditionAndOr(ConditionAndOr e) {
        importSet.add(ValueNull.class.getName());
        importSet.add(ValueBoolean.class.getName());
        Expression left = e.getLeft(), right = e.getRight();
        StringBuilder indent = indent((level + 1) * 4);

        buff.append(indent).append("{\r\n");
        String retVarLeft = "lret" + (level + 1);
        String retVarRight = "rret" + (level + 1);
        buff.append(indent).append("    Value ").append(retVarLeft).append(";\r\n");
        buff.append(indent).append("    Value ").append(retVarRight).append(";\r\n");
        left.accept(copy(level + 1, retVarLeft));

        switch (e.getAndOrType()) {
        case ConditionAndOr.AND:
            buff.append("    ").append(indent).append("if (!").append(retVarLeft).append(".getBoolean()) {\r\n");
            buff.append("    ").append(indent).append("    ").append(retVar).append(" = ").append(retVarLeft)
                    .append(";\r\n");
            buff.append("    ").append(indent).append("} else {\r\n");
            right.accept(copy(level + 2, retVarRight));
            buff.append("    ").append(indent).append("    if (!").append(retVarRight).append(".getBoolean()) {\r\n");
            buff.append("    ").append(indent).append("        ").append(retVar).append(" = ").append(retVarRight)
                    .append(";\r\n");
            buff.append("    ").append(indent).append("    } else if (").append(retVarLeft)
                    .append(" == ValueNull.INSTANCE) {\r\n");
            buff.append("    ").append(indent).append("        ").append(retVar).append(" = ").append(retVarLeft)
                    .append(";\r\n");
            buff.append("    ").append(indent).append("    } else if (").append(retVarRight)
                    .append(" == ValueNull.INSTANCE) {\r\n");
            buff.append("    ").append(indent).append("        ").append(retVar).append(" = ").append(retVarRight)
                    .append(";\r\n");
            buff.append("    ").append(indent).append("    } else {\r\n");
            buff.append("    ").append(indent).append("        ").append(retVar)
                    .append(" = ValueBoolean.get(true);\r\n");
            buff.append("    ").append(indent).append("    }\r\n");
            buff.append("    ").append(indent).append("}\r\n");
            break;
        case ConditionAndOr.OR:
            buff.append("    ").append(indent).append("if (").append(retVarLeft).append(".getBoolean()) {\r\n");
            buff.append("    ").append(indent).append("    ").append(retVar).append(" = ").append(retVarLeft)
                    .append(";\r\n");
            buff.append("    ").append(indent).append("} else {\r\n");
            right.accept(copy(level + 2, retVarRight));
            buff.append("    ").append(indent).append("    if (").append(retVarRight).append(".getBoolean()) {\r\n");
            buff.append("    ").append(indent).append("        ").append(retVar).append(" = ").append(retVarRight)
                    .append(";\r\n");
            buff.append("    ").append(indent).append("    } else if (").append(retVarLeft)
                    .append(" == ValueNull.INSTANCE) {\r\n");
            buff.append("    ").append(indent).append("        ").append(retVar).append(" = ").append(retVarLeft)
                    .append(";\r\n");
            buff.append("    ").append(indent).append("    } else if (").append(retVarRight)
                    .append(" == ValueNull.INSTANCE) {\r\n");
            buff.append("    ").append(indent).append("        ").append(retVar).append(" = ").append(retVarRight)
                    .append(";\r\n");
            buff.append("    ").append(indent).append("    } else {\r\n");
            buff.append("    ").append(indent).append("        ").append(retVar)
                    .append(" = ValueBoolean.get(false);\r\n");
            buff.append("    ").append(indent).append("    }\r\n");
            buff.append("    ").append(indent).append("}\r\n");
            break;
        }
        buff.append(indent).append("}").append("\r\n");
        return null;
    }

    @Override
    public Void visitConditionInConstantSet(ConditionInConstantSet e) {
        importSet.add(ValueNull.class.getName());
        importSet.add(ValueBoolean.class.getName());
        evaluator.setValueSet(e.getValueSet());

        StringBuilder indent = indent((level + 1) * 4);

        buff.append(indent).append("{\r\n");
        String retVarLeft = "lret" + (level + 1);
        buff.append(indent).append("    Value ").append(retVarLeft).append(";\r\n");
        e.getLeft().accept(copy(level + 1, retVarLeft));

        buff.append("    ").append(indent).append("if (").append(retVarLeft).append(" == ValueNull.INSTANCE) {\r\n");
        buff.append("    ").append(indent).append("    ").append(retVar).append(" = ").append(retVarLeft)
                .append(";\r\n");
        buff.append("    ").append(indent).append("} else if (evaluator.getValueSet().contains(").append(retVarLeft)
                .append(")) {\r\n");
        buff.append("    ").append(indent).append("    ").append(retVar).append(" = ValueBoolean.TRUE;\r\n");
        buff.append("    ").append(indent).append("} else {\r\n");
        buff.append("    ").append(indent).append("    ").append(retVar);
        if (e.getValueSet().contains(ValueNull.INSTANCE)) {
            buff.append(" = ValueNull.INSTANCE;\r\n");
        } else {
            buff.append(" = ValueBoolean.FALSE;\r\n");
        }
        buff.append("    ").append(indent).append("}\r\n");
        buff.append(indent).append("}").append("\r\n");
        return null;
    }

    @Override
    public Void visitConditionNot(ConditionNot e) {
        importSet.add(ValueNull.class.getName());
        importSet.add(Value.class.getName());

        StringBuilder indent = indent((level + 1) * 4);

        buff.append(indent).append("{\r\n");
        String retVarLeft = "lret" + (level + 1);
        buff.append(indent).append("    Value ").append(retVarLeft).append(";\r\n");
        e.getCondition().accept(copy(level + 1, retVarLeft));

        buff.append("    ").append(indent).append(retVar).append(" = ").append(retVarLeft)
                .append(" == ValueNull.INSTANCE ? ").append(retVarLeft).append(" : ").append(retVarLeft)
                .append(".convertTo(Value.BOOLEAN).negate();\r\n");
        buff.append(indent).append("}").append("\r\n");
        return null;
    }

    @Override
    public Void visitAlias(Alias e) {
        return visitExpression(e);
    }

    @Override
    public Void visitSequenceValue(SequenceValue e) {
        visitExpression(e);
        return null;
    }

    @Override
    public Void visitSubQuery(SubQuery e) {
        visitExpression(e);
        return null;
    }

    @Override
    public Void visitWildcard(Wildcard e) {
        visitExpression(e);
        return null;
    }

    @Override
    public Void visitCompareLike(CompareLike e) {
        visitExpression(e);
        return null;
    }

    @Override
    public Void visitConditionExists(ConditionExists e) {
        visitExpression(e);
        return null;
    }

    @Override
    public Void visitConditionIn(ConditionIn e) {
        visitExpression(e);
        return null;
    }

    @Override
    public Void visitConditionInSelect(ConditionInSelect e) {
        visitExpression(e);
        return null;
    }

    @Override
    public Void visitAggregate(Aggregate e) {
        visitExpression(e);
        return null;
    }

    @Override
    public Void visitAGroupConcat(AGroupConcat e) {
        visitExpression(e);
        return null;
    }

    @Override
    public Void visitJavaAggregate(JavaAggregate e) {
        visitExpression(e);
        return null;
    }

    @Override
    public Void visitFunction(Function e) {
        visitExpression(e);
        return null;
    }

    @Override
    public Void visitJavaFunction(JavaFunction e) {
        visitExpression(e);
        return null;
    }

    @Override
    public Void visitTableFunction(TableFunction e) {
        visitExpression(e);
        return null;
    }
}
