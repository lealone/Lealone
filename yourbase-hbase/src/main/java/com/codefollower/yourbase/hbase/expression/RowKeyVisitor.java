package com.codefollower.yourbase.hbase.expression;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.yourbase.command.dml.Select;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.expression.Comparison;
import com.codefollower.yourbase.expression.Expression;
import com.codefollower.yourbase.expression.ExpressionColumn;
import com.codefollower.yourbase.expression.Visitor;
import com.codefollower.yourbase.hbase.dbobject.table.HBaseTable;
import com.codefollower.yourbase.value.Value;

@SuppressWarnings("unused")
public class RowKeyVisitor extends Visitor<Expression, Expression> {
    private final String rowKeyName;
    private final byte[] tableName;
    private final Session session;

    private List<byte[]> startKeys;
    private byte[] startKey;
    private byte[] endKey;

    protected int compareTypeStart = -1;
    protected Value compareValueStart;
    protected int compareTypeStop = -1;
    protected Value compareValueStop;
    
    public RowKeyVisitor(HBaseTable table, Select select, Session session) {

        this.rowKeyName = table.getRowKeyName();
        this.tableName = Bytes.toBytes(table.getName());
        this.session = session;
    }


    @Override
    public Expression visitExpressionColumn(ExpressionColumn e, Expression s) {
        return visitExpression(e, s);
    }
    @Override
    public Expression visitComparison(Comparison e, Expression s) {
        Expression left = e.getExpression(true);
        Expression right = e.getExpression(false);
        int compareType = e.getCompareType();
        
        if (left instanceof ExpressionColumn && ((ExpressionColumn) left).getTableFilter().getTable().supportsColumnFamily()) {
            if (rowKeyName.equalsIgnoreCase(((ExpressionColumn) left).getColumnName())) {
                switch (compareType) {
                case Comparison.EQUAL:
                case Comparison.BIGGER_EQUAL:
                    compareTypeStart = compareType;
                    compareValueStart = right.getValue(session);
                    break;
                case Comparison.SMALLER:
                    compareTypeStop = compareType;
                    compareValueStop = right.getValue(session);
                    break;
                default:
                    throw new RuntimeException("rowKey compare type is not = or >= or <");
                }
                return null;
            }
        }

        left = left.accept(this, left);
        if (right != null)
            right = right.accept(this, right);
        if (left == null)
            return right;
        if (right == null)
            return left;

        return s;
    }

}
