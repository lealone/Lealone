/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import org.lealone.sql.operator.OperatorFactoryBase;

public class VOperatorFactory extends OperatorFactoryBase {

    public VOperatorFactory() {
        super("Vector");
    }

    @Override
    public VOperator createOperator(Select select) {
        if (select.isQuickAggregateQuery) {
            return null;
        } else if (select.isGroupQuery) {
            if (select.isGroupSortedQuery) {
                return null;
            } else {
                return null;
            }
        } else if (select.isDistinctQuery) {
            return null;
        } else {
            return new VFlat(select);
        }
    }

}
