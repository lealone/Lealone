/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.optimizer;

import java.util.ArrayList;
import java.util.Random;

import org.lealone.common.util.BitField;
import org.lealone.db.index.Index;
import org.lealone.db.result.SortOrder;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.sql.expression.Expression;

/**
 * The optimizer is responsible to find the best execution plan
 * for a given query.
 */
public class Optimizer {

    private static final int MAX_BRUTE_FORCE_FILTERS = 7;
    private static final int MAX_BRUTE_FORCE = 2000;
    private static final int MAX_GENETIC = 500;
    private long start;
    private BitField switched;

    // 后面的plan个数等于前面的plan个数乘以后面的filter数
    // 比如第三行是6，等于第三行的filter 3 * 2 = 6
    // possible plans for filters, if using brute force:
    // 1 filter 1 plan
    // 2 filters 2 plans
    // 3 filters 6 plans
    // 4 filters 24 plans
    // 5 filters 120 plans
    // 6 filters 720 plans
    // 7 filters 5040 plans
    // 8 filters 40320 plan
    // 9 filters 362880 plans
    // 10 filters 3628800 filters

    private final TableFilter[] filters;
    private final Expression condition;
    private final ServerSession session;

    private Plan bestPlan;
    private TableFilter topFilter;
    private double cost;
    private Random random;

    public Optimizer(TableFilter[] filters, Expression condition, ServerSession session) {
        this.filters = filters; // join时才有多个TableFilter，正常只有一个TableFilter
        this.condition = condition;
        this.session = session;
    }

    /**
     * How many filter to calculate using brute force. The remaining filters are
     * selected using a greedy algorithm which has a runtime of (1 + 2 + ... +
     * n) = (n * (n-1) / 2) for n filters. The brute force algorithm has a
     * runtime of n * (n-1) * ... * (n-m) when calculating m brute force of n
     * total. The combined runtime is (brute force) * (greedy).
     *
     * @param filterCount the number of filters total
     * @return the number of filters to calculate using brute force
     */
    private static int getMaxBruteForceFilters(int filterCount) {
        int i = 0, j = filterCount, total = filterCount;
        while (j > 0 && total * (j * (j - 1) / 2) < MAX_BRUTE_FORCE) {
            j--;
            total *= j;
            i++;
        }
        return i;
    }

    private void calculateBestPlan() {
        start = System.currentTimeMillis();
        cost = -1;
        if (filters.length == 1) {
            testPlan(filters);
        } else if (filters.length <= MAX_BRUTE_FORCE_FILTERS) {
            calculateBruteForceAll();
        } else {
            calculateBruteForceSome();
            random = new Random(0);
            calculateGenetic();
        }
    }

    private boolean canStop(int x) {
        if ((x & 127) == 0) {
            long t = System.currentTimeMillis() - start;
            // don't calculate for simple queries (no rows or so)
            if (cost >= 0 && 10 * t > cost) {
                return true;
            }
        }
        return false;
    }

    private void calculateBruteForceAll() {
        TableFilter[] list = new TableFilter[filters.length];
        Permutations<TableFilter> p = Permutations.create(filters, list);
        for (int x = 0; !canStop(x) && p.next(); x++) {
            testPlan(list);
        }
    }

    private void calculateBruteForceSome() {
        int bruteForce = getMaxBruteForceFilters(filters.length);
        TableFilter[] list = new TableFilter[filters.length];
        Permutations<TableFilter> p = Permutations.create(filters, list, bruteForce);
        for (int x = 0; !canStop(x) && p.next(); x++) {
            // find out what filters are not used yet
            for (TableFilter f : filters) {
                f.setUsed(false);
            }
            for (int i = 0; i < bruteForce; i++) {
                list[i].setUsed(true);
            }
            // fill the remaining elements with the unused elements (greedy)
            for (int i = bruteForce; i < filters.length; i++) {
                double costPart = -1.0;
                int bestPart = -1;
                for (int j = 0; j < filters.length; j++) {
                    if (!filters[j].isUsed()) {
                        if (i == filters.length - 1) {
                            bestPart = j;
                            break;
                        }
                        list[i] = filters[j];
                        Plan part = new Plan(list, i + 1, condition);
                        double costNow = part.calculateCost(session);
                        if (costPart < 0 || costNow < costPart) {
                            costPart = costNow;
                            bestPart = j;
                        }
                    }
                }
                filters[bestPart].setUsed(true);
                list[i] = filters[bestPart];
            }
            testPlan(list);
        }
    }

    private void calculateGenetic() {
        TableFilter[] best = new TableFilter[filters.length];
        TableFilter[] list = new TableFilter[filters.length];
        for (int x = 0; x < MAX_GENETIC; x++) {
            if (canStop(x)) {
                break;
            }
            boolean generateRandom = (x & 127) == 0;
            if (!generateRandom) {
                System.arraycopy(best, 0, list, 0, filters.length);
                if (!shuffleTwo(list)) {
                    generateRandom = true;
                }
            }
            if (generateRandom) {
                switched = new BitField();
                System.arraycopy(filters, 0, best, 0, filters.length);
                shuffleAll(best);
                System.arraycopy(best, 0, list, 0, filters.length);
            }
            if (testPlan(list)) {
                switched = new BitField();
                System.arraycopy(list, 0, best, 0, filters.length);
            }
        }
    }

    private boolean testPlan(TableFilter[] list) {
        Plan p = new Plan(list, list.length, condition);
        double costNow = p.calculateCost(session);
        if (cost < 0 || costNow < cost) {
            cost = costNow;
            bestPlan = p;
            return true;
        }
        return false;
    }

    private void shuffleAll(TableFilter[] f) { // 随机把数组中的两个元素兑换f.length-1次
        for (int i = 0; i < f.length - 1; i++) {
            int j = i + random.nextInt(f.length - i);
            if (j != i) {
                TableFilter temp = f[i];
                f[i] = f[j];
                f[j] = temp;
            }
        }
    }

    private boolean shuffleTwo(TableFilter[] f) { // 随机把数组中的两个元素兑换1次
        int a = 0, b = 0, i = 0;
        for (; i < 20; i++) {
            a = random.nextInt(f.length);
            b = random.nextInt(f.length);
            if (a == b) {
                continue;
            }
            if (a < b) {
                int temp = a;
                a = b;
                b = temp;
            }
            int s = a * f.length + b;
            if (switched.get(s)) {
                continue;
            }
            switched.set(s);
            break;
        }
        if (i == 20) {
            return false;
        }
        TableFilter temp = f[a];
        f[a] = f[b];
        f[b] = temp;
        return true;
    }

    /**
     * Calculate the best query plan to use.
     */
    public void optimize() {
        calculateBestPlan();
        bestPlan.removeUnusableIndexConditions();
        TableFilter[] f2 = bestPlan.getFilters();
        topFilter = f2[0];
        for (int i = 0; i < f2.length - 1; i++) {
            // 见Parser.parseJoinTableFilter(TableFilter, Select)中的注释
            f2[i].addJoin(f2[i + 1], false, null);
        }
        for (TableFilter f : f2) {
            PlanItem item = bestPlan.getItem(f);
            f.setPlanItem(item);
        }
    }

    public TableFilter getTopFilter() {
        return topFilter;
    }

    public double getCost() {
        return cost;
    }

    /**
     * Get the best plan for the given search mask.
     *
     * @param session the session
     * @param masks per-column comparison bit masks, null means 'always false',
     *              see constants in IndexCondition
     * @param sortOrder the sort order
     * @return the plan item
     */
    public static PlanItem getBestPlanItem(ServerSession session, int[] masks, Table table, SortOrder sortOrder) {
        PlanItem item = new PlanItem();
        item.setIndex(table.getScanIndex(session));
        item.cost = item.getIndex().getCost(session, null, null);
        ArrayList<Index> indexes = table.getIndexes();
        if (indexes != null && masks != null) {
            // indexes[0]是ScanIndex，所以可以跳过，从1开始
            for (int i = 1, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                double cost = index.getCost(session, masks, sortOrder);
                if (cost < item.cost) {
                    item.cost = cost;
                    item.setIndex(index);
                }
            }
        }
        return item;
    }
}
