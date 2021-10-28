/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import org.lealone.sql.operator.OperatorFactoryBase;

public class VOperatorFactory extends OperatorFactoryBase {

    public VOperatorFactory() {
        super("olap");
    }

    @Override
    public VOperator createOperator(Select select) {
        if (select.isQuickAggregateQuery) {
            return null;
        } else if (select.isGroupQuery) {
            if (select.isGroupSortedQuery) {
                return new VGroupSorted(select);
            } else {
                if (select.groupIndex == null) { // 忽视select.havingIndex
                    return new VAggregate(select);
                } else {
                    return new VGroup(select);
                }
            }
        } else if (select.isDistinctQuery) {
            return null;
        } else {
            return new VFlat(select);
        }
    }
}
