/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.util;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * bounded threadsafe deque
 */
public class BoundedStatsDeque implements Iterable<Long> {
    private final LinkedBlockingDeque<Long> deque;
    private final AtomicLong sum;

    public BoundedStatsDeque(int size) {
        deque = new LinkedBlockingDeque<>(size);
        sum = new AtomicLong(0);
    }

    @Override
    public Iterator<Long> iterator() {
        return deque.iterator();
    }

    public int size() {
        return deque.size();
    }

    public void add(long i) {
        if (!deque.offer(i)) {
            Long removed = deque.remove();
            sum.addAndGet(-removed);
            deque.offer(i);
        }
        sum.addAndGet(i);
    }

    public long sum() {
        return sum.get();
    }

    public double mean() {
        return size() > 0 ? ((double) sum()) / size() : 0;
    }
}
