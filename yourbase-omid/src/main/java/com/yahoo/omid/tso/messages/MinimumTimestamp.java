package com.yahoo.omid.tso.messages;

import java.io.Serializable;

public class MinimumTimestamp implements Serializable {
    private static final long serialVersionUID = -2189747376139262617L;

    private final long timestamp;

    public MinimumTimestamp(long timestamp) {
        super();
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "{MinimumTimestamp " + timestamp + "}";
    }

}
