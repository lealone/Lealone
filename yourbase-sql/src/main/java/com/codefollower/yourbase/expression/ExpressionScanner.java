package com.codefollower.yourbase.expression;

public class ExpressionScanner<R, S> extends Visitor<R, S> {
    public R scan(Expression e, S s) {
        return (e == null) ? null : e.accept(this, s);
    }

    public R scan(Iterable<? extends Expression> expressions, S s) {
        R r = null;
        if (expressions != null) {
            boolean first = true;
            for (Expression expression : expressions) {
                r = (first ? scan(expression, s) : scanAndReduce(expressions, s, r));
                first = false;
            }
        }
        return r;
    }

    private R scanAndReduce(Iterable<? extends Expression> expressions, S s, R r) {
        return reduce(scan(expressions, s), r);
    }

    public R reduce(R r1, R r2) {
        return r1;
    }

    
    public R visitComparison(Comparison e, S s) {
        return visitExpression(e, s);
    }

    public R visitExpressionColumn(ExpressionColumn e, S s) {
        return visitExpression(e, s);
    }
}
