package com.yahoo.omid.tso;

public class AbortedTransaction {
    private long startTimestamp;
    private long snapshot;

    public AbortedTransaction(long startTimestamp, long snapshot) {
        super();
        this.startTimestamp = startTimestamp;
        this.snapshot = snapshot;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(long snapshot) {
        this.snapshot = snapshot;
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
