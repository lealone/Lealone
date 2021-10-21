/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.optimizer;

import java.util.ArrayList;
import java.util.HashMap;

import org.lealone.db.session.ServerSession;
import org.lealone.sql.expression.Expression;

/**
 * A possible query execution plan. The time required to execute a query depends
 * on the order the tables are accessed.
 * 
 * @author H2 Group
 * @author zhh
 */
public class Plan {

    private final TableFilter[] filters;
    private final HashMap<TableFilter, PlanItem> planItems = new HashMap<>();
    private final TableFilter[] allFilters;

    /**
     * Create a query plan with the given order.
     *
     * @param filters the tables of the query
     * @param count the number of table items
     */
    public Plan(TableFilter[] filters, int count) {
        this.filters = new TableFilter[count];
        System.arraycopy(filters, 0, this.filters, 0, count);
        final ArrayList<TableFilter> all = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            filters[i].visit(f -> all.add(f));
        }
        allFilters = all.toArray(new TableFilter[0]);
    }

    /**
     * Get the plan item for the given table.
     *
     * @param filter the table
     * @return the plan item
     */
    public PlanItem getItem(TableFilter filter) {
        return planItems.get(filter);
    }

    /**
     * The the list of tables.
     *
     * @return the list of tables
     */
    public TableFilter[] getFilters() {
        return filters;
    }

    /**
     * Optimize full conditions and remove all index conditions that can not be used.
     */
    public void optimizeConditions() {
        for (int i = 0; i < allFilters.length; i++) {
            TableFilter f = allFilters[i];
            setEvaluatable(f, true);
            if (i < allFilters.length - 1) {
                // the last table doesn't need the optimization,
                // otherwise the expression is calculated twice unnecessarily
                // (not that bad but not optimal)
                f.optimizeFullCondition();
            }
            f.removeUnusableIndexConditions();
        }
        for (TableFilter f : allFilters) {
            setEvaluatable(f, false);
        }
    }

    /**
     * Calculate the cost of this query plan.
     *
     * @param session the session
     * @return the cost
     */
    public double calculateCost(ServerSession session) {
        double cost = 1;
        boolean invalidPlan = false;
        int level = 1;
        for (TableFilter tableFilter : allFilters) {
            PlanItem item = tableFilter.getBestPlanItem(session, level++);
            planItems.put(tableFilter, item);
            cost += cost * item.cost;
            setEvaluatable(tableFilter, true);
            Expression on = tableFilter.getJoinCondition();
            if (on != null && !on.isEvaluatable()) {
                invalidPlan = true;
                break;
            }
        }
        if (invalidPlan) {
            cost = Double.POSITIVE_INFINITY;
        }
        for (TableFilter f : allFilters) {
            setEvaluatable(f, false);
        }
        return cost;
    }

    private void setEvaluatable(TableFilter filter, boolean b) {
        filter.setEvaluatable(filter, b);
    }
}
