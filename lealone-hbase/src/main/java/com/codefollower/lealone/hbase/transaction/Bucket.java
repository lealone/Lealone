package com.codefollower.lealone.hbase.transaction;

import java.util.BitSet;
import java.util.Set;
import java.util.TreeSet;

public class Bucket {
    private static final long BUCKET_SIZE = 32768; // 2 ^ 15

    private final BitSet transactions = new BitSet((int) BUCKET_SIZE);
    private final int position;

    private int transactionsCommited = 0;
    private int firstUncommited = 0;
    private boolean closed = false;

    public Bucket(int position) {
        this.position = position;
    }

    public boolean isUncommited(long id) {
        return !transactions.get((int) (id % BUCKET_SIZE));
    }

    public Set<Long> abortAllUncommited() {
        Set<Long> result = abortUncommited(BUCKET_SIZE - 1);
        closed = true;
        return result;
    }

    public synchronized Set<Long> abortUncommited(long id) {
        int lastCommited = (int) (id % BUCKET_SIZE);

        Set<Long> aborted = new TreeSet<Long>();
        if (allCommited()) {
            return aborted;
        }

        for (int i = transactions.nextClearBit(firstUncommited); i >= 0 && i <= lastCommited; i = transactions
                .nextClearBit(i + 1)) {
            aborted.add(((long) position) * BUCKET_SIZE + i);
            commit(i);
        }

        firstUncommited = lastCommited + 1;

        return aborted;
    }

    public synchronized void commit(long id) {
        transactions.set((int) (id % BUCKET_SIZE));
        ++transactionsCommited;
    }

    public boolean allCommited() {
        return BUCKET_SIZE == transactionsCommited || closed;
    }

    public static long getBucketSize() {
        return BUCKET_SIZE;
    }

    public long getFirstUncommitted() {
        return position * BUCKET_SIZE + firstUncommited;
    }

}
