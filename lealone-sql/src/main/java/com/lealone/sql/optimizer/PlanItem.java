/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.optimizer;

import com.lealone.db.index.Index;

/**
 * The plan item describes the index to be used, and the estimated cost when using it.
 * 
 * @author H2 Group
 * @author zhh
 */
public class PlanItem {

    /**
     * The cost.
     */
    double cost;

    private Index index;
    private PlanItem joinPlan;
    private PlanItem nestedJoinPlan;

    public double getCost() {
        return cost;
    }

    void setIndex(Index index) {
        this.index = index;
    }

    public Index getIndex() {
        return index;
    }

    void setJoinPlan(PlanItem joinPlan) {
        this.joinPlan = joinPlan;
    }

    PlanItem getJoinPlan() {
        return joinPlan;
    }

    void setNestedJoinPlan(PlanItem nestedJoinPlan) {
        this.nestedJoinPlan = nestedJoinPlan;
    }

    PlanItem getNestedJoinPlan() {
        return nestedJoinPlan;
    }
}
