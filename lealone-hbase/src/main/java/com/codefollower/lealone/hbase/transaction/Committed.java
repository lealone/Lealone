package com.codefollower.lealone.hbase.transaction;

import java.util.Arrays;

public class Committed {

    private static final int BKT_NUMBER = 1 << 15;

    private CommitBucket buckets[] = new CommitBucket[BKT_NUMBER];
    private int firstCommitedBucket = 0;
    private int lastOpenedBucket = 0;

    public Committed() {
    }

    public synchronized void commit(long id, long timestamp) {
        int position = getPosition(id);
        CommitBucket bucket = buckets[position];
        if (bucket == null) {
            bucket = new CommitBucket();
            buckets[position] = bucket;
            lastOpenedBucket = position;
        }
        bucket.commit(id, timestamp);
    }

    public long getCommit(long id) {
        CommitBucket bucket = buckets[getPosition(id)];
        if (bucket == null) {
            return -1;
        }
        return bucket.getCommit(id);
    }

    public void raiseLargestDeletedTransaction(long id) {
        int maxBucket = getPosition(id);
        while (firstCommitedBucket != maxBucket && firstCommitedBucket != lastOpenedBucket) {
            buckets[firstCommitedBucket] = null;
            firstCommitedBucket = (firstCommitedBucket + 1) % BKT_NUMBER;
        }
    }

    private int getPosition(long id) {
        return ((int) (id / CommitBucket.getBucketSize())) % BKT_NUMBER;
    }

    public long getSize() {
        return BKT_NUMBER * 8 + (lastOpenedBucket - firstCommitedBucket) * CommitBucket.BUCKET_SIZE * 8;
    }
}

class CommitBucket {

    static final long BUCKET_SIZE = 1 << 14;

    private long transactions[] = new long[(int) BUCKET_SIZE];

    public CommitBucket() {
        Arrays.fill(transactions, -1);
    }

    public long getCommit(long id) {
        return transactions[(int) (id % BUCKET_SIZE)];
    }

    public void commit(long id, long timestamp) {
        transactions[(int) (id % BUCKET_SIZE)] = timestamp;
    }

    public static long getBucketSize() {
        return BUCKET_SIZE;
    }

}
