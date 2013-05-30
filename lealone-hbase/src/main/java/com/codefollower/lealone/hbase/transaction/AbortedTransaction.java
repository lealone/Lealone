package com.codefollower.lealone.hbase.transaction;

public class AbortedTransaction {
    private final long startTimestamp;
    private final long snapshot;

    public AbortedTransaction(long startTimestamp, long snapshot) {
        this.startTimestamp = startTimestamp;
        this.snapshot = snapshot;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getSnapshot() {
        return snapshot;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (startTimestamp ^ (startTimestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AbortedTransaction other = (AbortedTransaction) obj;
        if (startTimestamp != other.startTimestamp)
            return false;
        return true;
    }

}
